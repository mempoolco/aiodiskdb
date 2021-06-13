import asyncio
import hashlib
import os
import shutil
from collections import OrderedDict
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import time
import typing

from aiodiskdb import exceptions
from aiodiskdb.internals import ensure_running, ensure_async_lock, logger
from aiodiskdb.abstracts import AsyncRunnable, AioDiskDBTransactionAbstract
from aiodiskdb.local_types import ItemLocation, LockType, Buffer, TempBufferData, WriteEvent, FileHeader

_FILE_SIZE = 128
_FILE_PREFIX = 'data'
_FILE_ZEROS_PADDING = 5
_BUFFER_SIZE = 16
_BUFFER_ITEMS = 100
_FLUSH_INTERVAL = 30
_GENESIS_BYTES = b'\r\xce\x8f7'
_TIMEOUT = 30
_CONCURRENCY = 32


class AioDiskDB(AsyncRunnable):
    GENESIS_BYTES_LENGTH = 4
    HEADER_TRIM_OFFSET = 4
    RESERVED_HEADER_LENGTH = 16  # for future usage

    """
    Minimal on-disk DB, with buffers and timeouts.
    Made with love for Asyncio.
    """
    def __init__(
            self,
            path: str,
            create_if_not_exists: bool = False,
            overwrite: bool = False,
            clean_stale_data: bool = True,
            file_padding: int = _FILE_ZEROS_PADDING,
            file_prefix: str = _FILE_PREFIX,
            max_file_size: int = _FILE_SIZE,
            max_buffer_items: int = _BUFFER_ITEMS,
            max_buffer_size: int = _BUFFER_SIZE,
            flush_interval: int = _FLUSH_INTERVAL,
            genesis_bytes: bytes = _GENESIS_BYTES,
            timeout: int = _TIMEOUT,
            concurrency: int = _CONCURRENCY
    ):
        super().__init__()
        if not file_prefix.isalpha():
            raise exceptions.InvalidConfigurationException('Wrong file prefix (must be alphabetic string)')
        self.path = Path(path)
        if create_if_not_exists:
            self.path.mkdir(parents=True, exist_ok=True)
        self._file_prefix = file_prefix
        if max_file_size <= 0 or max_buffer_size <= 0:
            raise exceptions.InvalidConfigurationException('max file size and max buffer size must be > 0')
        if max_file_size < max_buffer_size:
            raise exceptions.InvalidConfigurationException('max_file_size must be >= max_buffer_size')
        self._file_padding = int(file_padding)
        self._max_file_size = int(float(max_file_size) * 1024 ** 2)
        self._max_buffer_items = int(max_buffer_items)
        self._max_buffer_size = int(float(max_buffer_size) * 1024 ** 2)
        self._flush_interval = int(flush_interval)
        if len(genesis_bytes) != self.GENESIS_BYTES_LENGTH:
            raise exceptions.InvalidConfigurationException('Genesis bytes length must be 4')
        self._genesis_bytes = genesis_bytes
        self._timeout = int(timeout)
        self._buffer_index = OrderedDict()
        self._buffers: typing.List[Buffer] = list()
        self._last_flush = None
        self._executor = ThreadPoolExecutor(max_workers=concurrency)
        self._overwrite = overwrite
        self._tmp_idx_and_buffer = TempBufferData(idx=dict(), buffer=None)
        self._file_header_size = self.GENESIS_BYTES_LENGTH + \
                             self.HEADER_TRIM_OFFSET + \
                             self.RESERVED_HEADER_LENGTH
        if clean_stale_data:
            self._drop_existing_temp_files()
            self._apply_checkpoint()
        else:
            self._ensure_no_pending_checkpoint()
        _max_accepted_file_size = 2**32 - 1 - self._file_header_size
        if max_file_size > _max_accepted_file_size:
            raise exceptions.InvalidConfigurationException(f'max file size is {_max_accepted_file_size}b')

    def _hash_file(self, f: typing.IO) -> typing.Optional[bytes]:
        """
        Hash files chunk by chunk, avoid to load the whole file in RAM.
        """
        i = 0
        chunk_size = 1024 ** 2
        _hash = None
        while 1:
            f.seek(i)
            c = f.read(chunk_size)
            if not c:
                break
            i += chunk_size
            _hash = hashlib.sha256().digest()
            f.seek(0)

        return _hash

    def _bake_new_file_header(self) -> bytes:
        """
        Bake a fresh file header for a new database file.
        """
        return FileHeader(
            genesis_bytes=self._genesis_bytes,
            trim_offset=0,
        ).serialize()

    def _read_file_header(self, f: typing.IO):
        """
        Read the first bytes of a file, and return the FileHeader
        """
        f.seek(0)
        header = f.read(self._file_header_size)
        if not self._is_file_header(header):
            raise exceptions.InvalidDataFileException
        return FileHeader(
            genesis_bytes=self._genesis_bytes,
            trim_offset=int.from_bytes(
                header[self.GENESIS_BYTES_LENGTH:self.GENESIS_BYTES_LENGTH+self.HEADER_TRIM_OFFSET],
                'little'
            )
        )

    def _bake_new_buffer(self, index: int):
        """
        Bake a fresh buffer, for a new database file.
        """
        return Buffer(
            index=index,
            data=b'',
            size=0,
            items=0,
            file_size=0,
            offset=0,
            head=True
        )

    async def _refresh_current_buffer(self):
        """
        Reload the current session buffer from the system state.
        To be used after a transaction or an index change (trim\drop).
        """
        self._buffer_index.pop(self._buffers[-1].index)
        self._buffers = []
        await self._setup_current_buffer()

    def _pre_stop_signal(self) -> bool:
        """
        Handle graceful stop signals. Flush buffer to disk.
        """
        if self._blocking_stop:
            return False
        self._blocking_stop = True
        assert not self._tmp_idx_and_buffer.idx
        self._tmp_idx_and_buffer = None
        while self._buffer_index:
            buffer = self._buffers.pop(0)
            v = self._buffer_index.pop(buffer.index)
            self._save_buffer_to_disk(TempBufferData(idx={buffer.index: v}, buffer=buffer))
        return True

    @ensure_async_lock(LockType.WRITE)
    async def _clean_temp_buffer(self):
        """
        Cleanup the current temp buffer. Unload the RAM, to be triggered after a disk flush.
        """
        self._tmp_idx_and_buffer = TempBufferData(idx=dict(), buffer=None)

    def _read_data_from_buffer(self, location: ItemLocation):
        """
        Read data from the main buffer.
        Data is here until the flush_buffer task triggers a disk flush.
        """
        try:
            idx = self._buffer_index[location.index][location.position]
            buffer = self._buffers[idx]
        except KeyError:
            return None

        buffer_shift = 0 if buffer.size == buffer.file_size else buffer.file_size - buffer.size
        relative_position = location.position - buffer_shift
        data = buffer.data[relative_position: relative_position + location.size]
        assert len(data) == location.size, f'{len(data)} != {location.size}'
        return data

    def _read_data_from_temp_buffer(self, location: ItemLocation):
        """
        Data is placed into the temp buffer while saving it on disk,
        so that writes are non blocking.
        """
        try:
            idx = self._tmp_idx_and_buffer.idx[location.index][location.position]
        except KeyError:
            return None

        buffer = self._tmp_idx_and_buffer.buffer
        # just ensures the idx was previously saved, the temp buffer is flat.
        buffer_shift = 0 if buffer.size == buffer.file_size else buffer.file_size - buffer.size
        relative_position = location.position - buffer_shift
        data = idx is not None and buffer.data[relative_position:relative_position + location.size] or None
        assert data is not None and len(data) == location.size or data is None
        return data

    def _get_filename_by_idx(self, idx: int, temp=False) -> str:
        t = temp and '.tmp.' or ''
        return f'{self.path}/{t}{self._file_prefix}' + f'{idx}'.zfill(self._file_padding) + '.dat'

    async def _setup_current_buffer(self):
        """
        Setup the current buffer, starting from the disk files.
        If no files are found, setup a fresh buffer, otherwise check the genesis bytes.
        """
        logger.debug('Setting up current buffer')
        files = sorted(os.listdir(self.path))
        last_file = files and list(
            filter(lambda x: x.startswith(self._file_prefix) and x.endswith('.dat'), files)
        )[-1]
        if not last_file:
            # No previous files found for the current setup. Starting a new database.
            buffer = self._bake_new_buffer(0)
            offset, curr_idx = 0, 0
        else:
            curr_idx = int(last_file.replace(self._file_prefix, '').replace('.dat', ''))
            filename = self._get_filename_by_idx(curr_idx)
            curr_size = os.path.getsize(filename) - self._file_header_size
            if curr_size < 0:
                raise exceptions.InvalidDataFileException('Invalid file size')
            with open(filename, 'rb') as f:
                header = self._read_file_header(f)
                offset = header.trim_offset
            data = b''
            buffer = Buffer(
                index=curr_idx, data=data, size=len(data),
                items=0, file_size=curr_size, offset=offset,
                head=False
            )
        self._buffers.append(buffer)
        self._buffer_index[curr_idx] = OrderedDict()
        logger.debug('Current buffer setup done')

    @ensure_async_lock(LockType.TRANSACTION)
    async def _teardown(self):
        self._buffers[-1].size and await self._flush_buffer_no_transaction_lock()
        self._executor.shutdown(wait=True)

    def enable_overwrite(self):
        self._overwrite = True

    def disable_overwrite(self):
        self._overwrite = False

    @ensure_async_lock(LockType.WRITE)
    async def _pop_buffer_data(self) -> TempBufferData:
        """
        Remove the buffer from the data queue.
        Put it into the temp storage for disk writing.
        Allocate a new buffer for the data queue.
        """
        assert not self._tmp_idx_and_buffer.buffer, 'wrong state, cannot recover. buffer lost.'
        buffer = self._buffers.pop(0)
        v = self._buffer_index.pop(buffer.index)
        self._tmp_idx_and_buffer = TempBufferData(idx={buffer.index: v}, buffer=buffer)
        if buffer.file_size > self._max_file_size:
            new_buffer = self._bake_new_buffer(buffer.index + 1)
        else:
            new_buffer = Buffer(
                index=buffer.index, data=b'',
                size=0, items=0, file_size=buffer.file_size,
                offset=buffer.offset, head=False
            )
        self._buffers.append(new_buffer)
        self._buffer_index[new_buffer.index] = OrderedDict()
        return self._tmp_idx_and_buffer

    def _save_buffer_to_disk(self, buffer_data: TempBufferData):
        """
        Actually saves data from a temp buffer to the target file and position.
        """
        logger.debug('Saving buffer to disk')
        assert buffer_data.buffer and buffer_data.idx, (buffer_data.buffer, buffer_data.idx)
        buffer = buffer_data.buffer
        filename = self._get_filename_by_idx(buffer.index)
        try:
            if buffer.head:
                if Path(filename).exists():
                    self._stop = True
                    raise exceptions.FilesInconsistencyException(f'File {filename} should not exists.')

            elif os.path.getsize(filename) != buffer.file_size - buffer.size + self._file_header_size:
                self._stop = True
                raise exceptions.InvalidDataFileException(f'File {filename} has unexpected size.')

            with open(filename, 'ab') as file:
                if buffer.head:
                    file.write(self._bake_new_file_header())
                file.write(buffer.data)
        except FileNotFoundError:
            self._stop = True
            raise exceptions.FilesInconsistencyException(f'Missing file {filename}')

    def _is_file_header(self, data: bytes) -> bool:
        """
        Verify the header correctness
        """
        if len(data) != self._file_header_size:
            return False
        p2 = self.GENESIS_BYTES_LENGTH + self.HEADER_TRIM_OFFSET
        if not data[:self.GENESIS_BYTES_LENGTH] == self._genesis_bytes:
            return False
        if not int.from_bytes(
                data[self.GENESIS_BYTES_LENGTH:p2], 'little'
        ) < self._max_file_size - self._file_header_size:
            return False
        if not data[p2:p2+self.RESERVED_HEADER_LENGTH] == int(0).to_bytes(self.RESERVED_HEADER_LENGTH, 'little'):
            return False
        return True

    @ensure_async_lock(LockType.TRANSACTION)
    async def flush(self):
        logger.debug('Requested explicit flush')
        if not self._buffers[-1].data:
            return
        return await self._flush_buffer_no_transaction_lock()

    @ensure_async_lock(LockType.TRANSACTION)
    async def _flush_buffer(self):
        """
        Trigger blocking\non-blocking operations.
        - pop_buffer: coroutine, locked for writing
        - save_buffer_to_disk: blocking threaded task, non locked
        - clean_temp_buffer: coroutine, locked for writing
        """
        await self._flush_buffer_no_transaction_lock()

    async def _flush_buffer_no_transaction_lock(self):
        timestamp = int(time.time()*1000)
        temp_buffer_data = await self._pop_buffer_data()
        await self._write_db_checkpoint(timestamp, temp_buffer_data.buffer.index)
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._save_buffer_to_disk,
            temp_buffer_data
        )
        flush_time = time.time()
        if self.events.on_write:
            offset = temp_buffer_data.buffer.offset
            asyncio.get_event_loop().create_task(
                self.events.on_write(
                    flush_time,
                    WriteEvent(
                        index=temp_buffer_data.buffer.index,
                        position=temp_buffer_data.buffer.file_size - temp_buffer_data.buffer.size + offset,
                        size=temp_buffer_data.buffer.size
                    )
                )
            )

        await self._clean_temp_buffer()
        self._last_flush = flush_time
        await self._clean_db_checkpoint(timestamp)

    def _process_location_read(self, location: ItemLocation):
        """
        Actually read data from files.
        """
        try:
            filename = self._get_filename_by_idx(location.index)
            with open(filename, 'rb') as file:
                header = self._read_file_header(file)
                if location.position < header.trim_offset:
                    return None
                file.seek(self._file_header_size + location.position - header.trim_offset)
                read = file.read(location.size)
            return read or None
        except FileNotFoundError:
            return None

    async def _pre_loop(self):
        if self._last_flush is None:
            self._last_flush = time.time()
        await self._setup_current_buffer()

    async def _run_loop(self):
        buffer = self._buffers[0]
        if buffer.file_size >= self._max_file_size:
            await self._flush_buffer()
        elif buffer.size >= self._max_buffer_size:
            await self._flush_buffer()
        elif time.time() - self._last_flush >= self._flush_interval:
            await self._flush_buffer()
        elif buffer.items >= self._max_buffer_items:
            await self._flush_buffer()

    @ensure_running(True)
    async def add(self, data: bytes) -> ItemLocation:
        """
        Enqueue data into a buffer, update the indexes for reads from RAM.

        :param data: bytes
        :return: ItemLocation(int, int, int)
        """
        logger.debug('Adding item to db, size: %s', len(data))
        if not data:
            raise exceptions.EmptyPayloadException
        s = time.time()
        while self._buffers[-1].file_size + self._buffers[-1].offset >= self._max_file_size \
                or self._buffers[-1].size >= self._max_buffer_size:
            # wait for the LRT to shift the buffers
            await asyncio.sleep(0.01)
            if time.time() - s > self._timeout:
                raise exceptions.TimeoutException
        return await self._add(data)

    @ensure_async_lock(LockType.WRITE)
    async def _add(self, data: bytes) -> ItemLocation:
        """
        Add data into the current buffer.
        """
        buffer = self._buffers[-1]
        data_size = len(data)
        if data_size > self._max_buffer_size:
            raise exceptions.WriteFailedException(
                f'File too big: {data_size} > {self._max_buffer_size}'
            )
        location = ItemLocation(
            index=buffer.index,
            position=buffer.file_size,
            size=data_size
        )
        self._buffer_index[location.index][location.position] = len(self._buffers) - 1
        buffer.data += data
        buffer.items += 1
        buffer.size += data_size
        buffer.file_size += data_size
        return location

    @ensure_running(True)
    @ensure_async_lock(LockType.READ)
    async def read(self, location: ItemLocation):
        """
        Reads data from the DB.
        If there's no data in RAM for the given location, try with a disk read.

        :param location: ItemLocation(int, int, int)
        :return: bytes
        """
        logger.debug('Reading location from db: %s', location)
        return self._read_data_from_buffer(location) or \
            self._read_data_from_temp_buffer(location) or \
            await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._process_location_read,
            location
        )

    @ensure_running(True)
    async def transaction(self) -> AioDiskDBTransactionAbstract:
        from aiodiskdb.transaction import AioDiskDBTransaction
        return AioDiskDBTransaction(self)

    def destroy_db(self):
        """
        Destroy the DB, clean the disk.
        """
        logger.warning('Requested DB destroy')
        if self.running:
            raise exceptions.RunningException('Database must be stopped before destroying it')
        if not self._overwrite:
            raise exceptions.ReadOnlyDatabaseException
        shutil.rmtree(self.path)
        return True

    @ensure_running(True)
    @ensure_async_lock(LockType.TRANSACTION)
    async def drop_index(self, index: int) -> int:
        """
        Drop a single index.
        Ensures a flush first.
        If the deleted index is the current one, setup it again from scratch.
        """
        logger.info('Requested index drop: %s', index)
        if not self._overwrite:
            raise exceptions.ReadOnlyDatabaseException
        assert not self._tmp_idx_and_buffer.buffer
        return await self._drop_index_non_locked(index)

    async def _drop_index_non_locked(self, index: int):
        filename = self._get_filename_by_idx(index)
        if not Path(filename).exists():
            raise exceptions.IndexDoesNotExist
        dropped_index_size = os.path.getsize(filename) - self._file_header_size
        os.remove(filename)
        if self._buffers[-1].index == index:
            await self._refresh_current_buffer()

        if self.events.on_index_drop:
            asyncio.get_event_loop().create_task(
                self.events.on_index_drop(time.time(), index, dropped_index_size)
            )
        return dropped_index_size

    @ensure_running(True, allow_stop_state=True)
    async def _write_db_checkpoint(self, timestamp: int, *files_indexes: int):
        """
        Persist the current status of files before appending data.
        It will be reverted in case of a commit failure.
        """
        checkpoints = self._get_checkpoints()
        if checkpoints:
            raise exceptions.InvalidDBStateException('Requested a checkpoint, but a checkpoint already exist')

        filenames = map(self._get_filename_by_idx, files_indexes)

        checkpoint = []
        for i, file_name in enumerate(filenames):
            try:
                file_size = os.path.getsize(file_name)
            except FileNotFoundError:
                continue
            with open(file_name, 'rb') as f:
                file_hash = self._hash_file(f)
                f.seek(max(0, file_size - 8))
                last_file_bytes = f.read(8)
            checkpoint.append(
                [
                    files_indexes[i].to_bytes(6, 'little'),
                    file_size.to_bytes(6, 'little'),
                    last_file_bytes,
                    file_hash
                ]
            )
        with open(os.path.join(self.path, f'.checkpoint-{timestamp}'), 'wb') as f:
            for s in checkpoint:
                f.write(b''.join(s))

    @ensure_running(True, allow_stop_state=True)
    async def _clean_db_checkpoint(self, timestamp: int):
        """
        The transaction is successfully committed.
        The checkpoint file must be deleted.
        """
        try:
            os.remove(os.path.join(self.path, f'.checkpoint-{timestamp}'))
        except FileNotFoundError as e:
            raise exceptions.InvalidDBStateException(
                'Requested to delete a checkpoint that does not exist'
            ) from e

    def _get_checkpoints(self) -> typing.List[str]:
        checkpoints = list(
            filter(
                lambda x: x.startswith('.checkpoint-'),
                os.listdir(self.path)
            )
        )
        if len(checkpoints) > 1:
            # This should NEVER happen.
            raise exceptions.InvalidDBStateException('Multiple checkpoints, unrecoverable error.')
        return checkpoints

    def _apply_checkpoint(self):
        """
        Apply a database checkpoint to recover a previous state.
        A checkpoint is created before a transaction commit is made.
        A checkpoint is created before a transaction commit is made.
        Having a checkpoint file during the AioDiskDB boot mean that a transaction commit is failed,
        and stale data may exists in the files.
        Applying the checkpoint discard all the data added to the files after taking it.
        Stale data is deleted.
        """
        assert not self.running
        checkpoints = self._get_checkpoints()
        if not checkpoints:
            return
        checkpoint_file = checkpoints[0]
        checkpoint_filename = os.path.join(str(self.path), checkpoint_file)
        with open(checkpoint_filename, 'rb') as f:
            checkpoint = f.read()
        assert checkpoint
        pos = 0
        files = []
        while pos < len(checkpoint):
            files.append(
                [
                    int.from_bytes(checkpoint[pos:pos+6], 'little'),
                    int.from_bytes(checkpoint[pos+6:pos+12], 'little'),
                    checkpoint[pos+12:pos+20],
                    checkpoint[pos+20:pos+52]
                ]
            )
            pos += 52
        for file_data in files:
            self._recover_files_from_checkpoint(file_data)
        os.remove(checkpoint_filename)

    def _recover_files_from_checkpoint(self, file_data: typing.List):
        """
        Actually apply checkpoint rules to existing files.
        """
        file_id = file_data[0]
        checkpoint_file_size = file_data[1]
        checkpoint_file_last_bytes = file_data[2]
        expected_hash = file_data[3]
        origin_file_name = self._get_filename_by_idx(file_id)
        bkp_file_name = self._get_filename_by_idx(file_id, temp=True)
        shutil.copy(origin_file_name, bkp_file_name)
        exc = None
        with open(bkp_file_name, 'r+b') as f:
            header = f.read(self._file_header_size)
            if not self._is_file_header(header):
                exc = exceptions.InvalidDataFileException('Invalid file header')
            else:
                f.seek(checkpoint_file_size - len(checkpoint_file_last_bytes))
                data = f.read(len(checkpoint_file_last_bytes))
                f.seek(0)
                f.truncate(checkpoint_file_size)
                if data != checkpoint_file_last_bytes:
                    exc = exceptions.InvalidDataFileException('File corrupted. Unrecoverable error')
                final_hash = self._hash_file(f)
                if final_hash != expected_hash:
                    exc = exceptions.InvalidDataFileException('Invalid file hash recovered')
        if not exc:
            shutil.copy(bkp_file_name, origin_file_name)
        os.remove(bkp_file_name)
        if exc:
            raise exc

    def _drop_existing_temp_files(self):
        for file in filter(
            lambda x: x.startswith(f'.tmp.{self._file_prefix}') and x.endswith('.dat'),
            os.listdir(self.path)
        ):
            os.remove(os.path.join(str(self.path), file))

    def _ensure_no_pending_checkpoint(self):
        assert not self.running
        if any(map(
            lambda x: x.startswith('.checkpoint-'),
            os.listdir(self.path)
        )):
            raise exceptions.InvalidDBStateException('Pending checkpoint. DB must be cleaned.')

    @ensure_running(True)
    @ensure_async_lock(LockType.TRANSACTION)
    async def rtrim(self, index: int, trim_from: int, safety_check: bytes = b'') -> int:
        """
        Trim an index, from the right.

        trim_from: the index location from which to delete data.
        safety_check: optional, must match the first bytes of the trimmed slice.

        return the size of the trimmed slice.
        """
        logger.debug('Requested rtrim: %s, %s, %s', index, trim_from, safety_check)
        if not self._overwrite:
            raise exceptions.ReadOnlyDatabaseException
        assert not self._tmp_idx_and_buffer.buffer, self._tmp_idx_and_buffer.buffer
        try:
            await self._flush_buffer_no_transaction_lock()
            return await self._do_rtrim(index, trim_from, safety_check)
        except FileNotFoundError:
            raise exceptions.IndexDoesNotExist

    async def _do_rtrim(self, index: int, trim_from: int, safety_check: bytes) -> int:
        if index < 0:
            raise exceptions.IndexDoesNotExist('Index must be > 0')
        filename = self._get_filename_by_idx(index)
        assert isinstance(trim_from, int)
        if not trim_from and not safety_check:
            return await self._drop_index_non_locked(index)

        elif not trim_from:
            with open(filename, 'r+b') as f:
                if safety_check != f.read(len(safety_check)):
                    raise exceptions.InvalidTrimCommandException('safety check failed')
            return await self._drop_index_non_locked(index)

        pre_trim_file_size = os.path.getsize(filename)
        with open(filename, 'r+b') as f:
            f.seek(trim_from + self._file_header_size)
            if safety_check:
                if safety_check != f.read(len(safety_check)):
                    raise exceptions.InvalidTrimCommandException('safety check failed')
            f.seek(trim_from + self._file_header_size)
            f.truncate()
        file_size = os.path.getsize(filename)
        if self._buffers[-1].index == index:
            await self._refresh_current_buffer()
        return pre_trim_file_size - file_size

    @ensure_running(True)
    @ensure_async_lock(LockType.TRANSACTION)
    async def ltrim(self, index: int, trim_to: int, safety_check: bytes = b''):
        """
        Trim an index, from the left.

        trim_to: the index location of data that are going to be kept,
        anything before this point is trimmed out.
        safety_check: optional, must match the last bytes of the trimmed slice.
        """
        logger.debug('Requested ltrim: %s, %s, %s', index, trim_to, safety_check)
        if not self._overwrite:
            raise exceptions.ReadOnlyDatabaseException

        if index < 0:
            raise exceptions.InvalidDataFileException(
                'Index must be >= 0'
            )
        assert not self._tmp_idx_and_buffer.buffer, self._tmp_idx_and_buffer.buffer
        filename = self._get_filename_by_idx(index)
        temp_filename = self._get_filename_by_idx(index, temp=True)
        try:
            os.path.getsize(temp_filename)
            exc = exceptions.InvalidDBStateException('trim file already exists')
            self._error = exc
            self._stop = True
        except FileNotFoundError:
            pass
        try:
            await self._flush_buffer_no_transaction_lock()
            with open(filename, 'rb') as origin:
                header = self._read_file_header(origin)
            if trim_to <= header.trim_offset:
                raise exceptions.InvalidTrimCommandException(
                    f'trim_to must be > current_offset ({header.trim_offset})'
                )

            await self._do_ltrim(index, trim_to, safety_check, header)
            if self._buffers[-1].index == index:
                await self._refresh_current_buffer()
        except FileNotFoundError:
            raise exceptions.IndexDoesNotExist

    async def _do_ltrim(self, index: int, trim_to: int, safety_check: bytes, header: FileHeader) -> int:
        filename = self._get_filename_by_idx(index)
        index_size = os.path.getsize(filename) - self._file_header_size + header.trim_offset
        if index_size < trim_to:
            raise exceptions.InvalidTrimCommandException('trim_to must be <= index_size')
        elif index_size == trim_to:
            return await self._drop_index_non_locked(index)

        with open(filename, 'rb') as origin:
            safety_check_length = len(safety_check)
            seek_at = self._file_header_size + trim_to - header.trim_offset
            if safety_check_length:
                origin.seek(seek_at - safety_check_length)
                check = origin.read(safety_check_length)
                if check != safety_check:
                    raise exceptions.InvalidTrimCommandException('safety check failed')
            else:
                origin.seek(seek_at)
            temp_filename = self._get_filename_by_idx(index, temp=True)
            with open(temp_filename, 'wb') as target:
                header.trim_offset = trim_to
                target.write(header.serialize())
                data = origin.read(1024**2)
                while data:
                    target.write(data)
                    data = origin.read(1024 ** 2)
        shutil.move(temp_filename, filename)
        return trim_to
