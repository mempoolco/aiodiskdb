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
from aiodiskdb.internals import ensure_running,  ensure_async_lock
from aiodiskdb.abstracts import AsyncRunnable, AioDiskDBTransactionAbstract
from aiodiskdb.local_types import ItemLocation, LockType, Buffer, TempBufferData, WriteEvent

_FILE_SIZE = 128
_FILE_PREFIX = 'data'
_FILE_ZEROS_PADDING = 5
_BUFFER_SIZE = 16
_BUFFER_ITEMS = 1000
_FLUSH_INTERVAL = 30
_GENESIS_BYTES = b'\r\xce\x8f7'
_TIMEOUT = 30
_CONCURRENCY = 32


class AioDiskDB(AsyncRunnable):
    GENESIS_BYTES_LENGTH = 4
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
        if clean_stale_data:
            self._drop_existing_temp_files()
            self._apply_snapshot()
        else:
            self._ensure_no_pending_snapshot()

    def _hash_file(self, f: typing.BinaryIO) -> typing.Optional[bytes]:
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

    def _pre_stop_signal(self) -> bool:
        """
        Handle graceful stop signals. Flush buffer to disk.
        """
        if self._blocking_stop:
            return False
        self._blocking_stop = True
        if self._tmp_idx_and_buffer.idx:
            self._save_buffer_to_disk(self._tmp_idx_and_buffer)
        self._tmp_idx_and_buffer = None
        while self._buffer_index:
            buffer = self._buffers.pop(0)
            v = self._buffer_index.pop(buffer.index)
            self._save_buffer_to_disk(TempBufferData(idx={buffer.index: v}, buffer=buffer))
        return True

    @ensure_async_lock(LockType.WRITE)
    async def _clean_temp_buffer(self):
        await self._clean_temp_buffer_non_locked()

    async def _clean_temp_buffer_non_locked(self):
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

        offset = 0 if buffer.size == buffer.file_size else buffer.file_size - buffer.size
        relative_position = location.position - offset
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
        offset = 0 if buffer.size == buffer.file_size else buffer.file_size - buffer.size
        relative_position = location.position - offset
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
        files = sorted(os.listdir(self.path))
        if any(filter(lambda _f: _f.startswith('.transaction-snapshot-'), files)):
            raise exceptions.PendingSnapshotException

        last_file = files and list(filter(lambda x: x.startswith(self._file_prefix), files))[-1]
        if not last_file:
            # No previous files found for the current setup. Starting a new database.
            data, curr_size, curr_idx = self._genesis_bytes, self.GENESIS_BYTES_LENGTH, 0
        else:
            curr_idx = int(last_file.replace(self._file_prefix, '').replace('.dat', ''))
            filename = self._get_filename_by_idx(curr_idx)
            curr_size = os.path.getsize(filename)
            with open(filename, 'rb') as f:
                if f.read(4) != self._genesis_bytes:
                    raise exceptions.InvalidDataFileException
            data = b''
        self._buffers.append(
            Buffer(index=curr_idx, data=data, size=len(data), items=0, file_size=curr_size)
        )
        self._buffer_index[curr_idx] = OrderedDict()

    async def _teardown(self):
        self._buffers[-1].size and await self._flush_buffer()
        self._executor.shutdown(wait=True)

    def enable_overwrite(self):
        self._overwrite = True

    def disable_overwrite(self):
        self._overwrite = False

    @ensure_async_lock(LockType.WRITE)
    async def _pop_buffer_data(self) -> TempBufferData:
        return await self._pop_buffer_data_non_locked()

    async def _pop_buffer_data_non_locked(self) -> TempBufferData:
        """
        Remove the buffer from the data queue.
        Put it into the temp storage for disk writing.
        Allocate a new buffer for the data queue.
        """
        if self._tmp_idx_and_buffer.buffer:
            raise exceptions.InvalidDBStateException('wrong state, cannot recover. buffer lost.')
        buffer = self._buffers.pop(0)
        v = self._buffer_index.pop(buffer.index)
        self._tmp_idx_and_buffer = TempBufferData(idx={buffer.index: v}, buffer=buffer)
        if buffer.file_size > self._max_file_size:
            new_buffer = Buffer(
                index=buffer.index + 1, data=self._genesis_bytes, size=self.GENESIS_BYTES_LENGTH,
                items=0, file_size=self.GENESIS_BYTES_LENGTH
            )
        else:
            new_buffer = Buffer(
                index=buffer.index, data=b'', size=0, items=0, file_size=buffer.file_size
            )
        self._buffers.append(new_buffer)
        self._buffer_index[new_buffer.index] = OrderedDict()
        return self._tmp_idx_and_buffer

    def _save_buffer_to_disk(self, buffer_data: TempBufferData):
        """
        Actually saves data from a temp buffer to the target file and position.
        """
        assert buffer_data.buffer and buffer_data.idx, (buffer_data.buffer, buffer_data.idx)
        buffer = buffer_data.buffer
        filename = self._get_filename_by_idx(buffer.index)
        try:
            if buffer.data.startswith(self._genesis_bytes) and \
                    buffer.file_size - buffer.size < self._max_file_size:
                if Path(filename).exists():
                    self._stop = True
                    raise exceptions.FilesInconsistencyException(f'File {filename} should not exists.')

            elif os.path.getsize(filename) != buffer.file_size - buffer.size:
                self._stop = True
                raise exceptions.InvalidDataFileException(f'File {filename} has unexpected size.')

            with open(filename, 'ab') as file:
                file.write(buffer.data)
        except FileNotFoundError:
            self._stop = True
            raise exceptions.FilesInconsistencyException(f'Missing file {filename}')

    async def _flush_buffer(self, lock=True):
        """
        Trigger blocking\non-blocking operations.
        - pop_buffer: coroutine, locked for writing
        - save_buffer_to_disk: blocking threaded task, non locked
        - clean_temp_buffer: coroutine, locked for writing
        """
        timestamp = int(time.time()*1000)
        if lock:
            temp_buffer_data = await self._pop_buffer_data()
        else:
            temp_buffer_data = await self._pop_buffer_data_non_locked()
        if not temp_buffer_data.buffer.size or temp_buffer_data.buffer.data == self._genesis_bytes:
            return
        await self._write_db_snapshot(timestamp, temp_buffer_data.buffer.index)
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._save_buffer_to_disk,
            temp_buffer_data
        )
        flush_time = time.time()
        if self.events.on_write:
            position = temp_buffer_data.buffer.file_size - temp_buffer_data.buffer.size
            size = temp_buffer_data.buffer.size
            asyncio.get_event_loop().create_task(
                self.events.on_write(
                    flush_time,
                    WriteEvent(
                        index=temp_buffer_data.buffer.index,
                        position=not position and self.GENESIS_BYTES_LENGTH or position,
                        size=not position and size - self.GENESIS_BYTES_LENGTH or size
                    )
                )
            )
        if lock:
            await self._clean_temp_buffer()
        else:
            await self._clean_temp_buffer_non_locked()

        self._last_flush = flush_time
        await self._clean_db_snapshot(timestamp)

    def _process_location_read(self, location: ItemLocation):
        """
        Actually read data from files.
        """
        try:
            filename = self._get_filename_by_idx(location.index)
            with open(filename, 'rb') as file:
                file.seek(location.position)
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
        if not data:
            raise exceptions.EmptyPayloadException
        s = time.time()
        while self._buffers[-1].file_size >= self._max_file_size \
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
        if data_size > self._max_file_size:
            raise exceptions.WriteFailedException(
                f'File too big: {data_size} > {self._max_file_size}'
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
        if self.running:
            raise exceptions.RunningException('Database must be stopped before destroying it')
        if not self._overwrite:
            raise exceptions.ReadOnlyDatabaseException
        shutil.rmtree(self.path)
        return True

    @ensure_running(True)
    @ensure_async_lock(LockType.WRITE)
    async def drop_index(self, index: int) -> int:
        """
        Destroy a single index.
        Ensures a flush first.
        If the deleted index is the current one, setup it again from scratch.
        """
        if not self._overwrite:
            raise exceptions.ReadOnlyDatabaseException
        if self._tmp_idx_and_buffer.buffer:
            await self._flush_buffer(lock=False)  # The whole method is locked.
        filename = self._get_filename_by_idx(index)
        if not Path(filename).exists():
            raise exceptions.IndexDoesNotExist
        dropped_index_size = os.path.getsize(filename) - self.GENESIS_BYTES_LENGTH
        os.remove(filename)

        if self._buffers[-1].index == index:
            self._buffers = []
            self._buffer_index.pop(index)
            await self._setup_current_buffer()

        if self.events.on_index_drop:
            asyncio.get_event_loop().create_task(
                self.events.on_index_drop(time.time(), index, dropped_index_size)
            )
        return dropped_index_size

    @ensure_running(True, allow_stop_state=True)
    async def _write_db_snapshot(self, timestamp: int, *files_indexes: int):
        """
        Persist the current status of files before appending data.
        It will be reverted in case of a commit failure.
        """
        filenames = map(self._get_filename_by_idx, files_indexes)
        snapshot = []
        for i, file_name in enumerate(filenames):
            try:
                file_size = os.path.getsize(file_name)
            except FileNotFoundError:
                continue
            with open(file_name, 'rb') as f:
                file_hash = self._hash_file(f)
                f.seek(max(0, file_size - 8))
                last_file_bytes = f.read(8)
            snapshot.append(
                [
                    files_indexes[i].to_bytes(6, 'little'),
                    file_size.to_bytes(6, 'little'),
                    last_file_bytes,
                    file_hash
                ]
            )
        with open(os.path.join(self.path, f'.snapshot-{timestamp}'), 'wb') as f:
            for s in snapshot:
                f.write(b''.join(s))

    @ensure_running(True, allow_stop_state=True)
    async def _clean_db_snapshot(self, timestamp: int):
        """
        The transaction is successfully committed.
        The snapshot file must be deleted.
        """
        try:
            os.remove(os.path.join(self.path, f'.snapshot-{timestamp}'))
        except FileNotFoundError:
            return

    def _apply_snapshot(self):
        """
        Apply a database snapshot to recover a previous state.
        A snapshot is created before a transaction commit is made.
        Having a snapshot file during the AioDiskDB boot mean that a transaction commit is failed,
        and stale data may exists in the files.
        Applying the snapshot discard all the data added to the files after taking it.
        Stale data is deleted.
        """
        assert not self.running
        snapshots = list(filter(
            lambda x: x.startswith('.snapshot-'),
            os.listdir(self.path)
        ))
        if len(snapshots) > 1:
            # This should NEVER happen.
            raise exceptions.InvalidDBStateException('Multiple snapshots, unrecoverable error.')
        if not snapshots:
            return
        snapshot_file = snapshots[0]
        snapshot_filename = os.path.join(str(self.path), snapshot_file)
        with open(snapshot_filename, 'rb') as f:
            snapshot = f.read()
        assert snapshot
        pos = 0
        files = []
        while pos < len(snapshot):
            files.append(
                [
                    int.from_bytes(snapshot[pos:pos+6], 'little'),
                    int.from_bytes(snapshot[pos+6:pos+12], 'little'),
                    snapshot[pos+12:pos+20],
                    snapshot[pos+20:pos+52]
                ]
            )
            pos += 52
        for file_data in files:
            self._recover_files_from_snapshot(file_data)
        os.remove(snapshot_filename)

    def _recover_files_from_snapshot(self, file_data: typing.List):
        """
        Actually apply snapshot rules to existing files.
        """
        file_id = file_data[0]
        snapshot_file_size = file_data[1]
        snapshot_file_last_bytes = file_data[2]
        expected_hash = file_data[3]
        origin_file_name = self._get_filename_by_idx(file_id)
        bkp_file_name = self._get_filename_by_idx(file_id, temp=True)
        shutil.copy(origin_file_name, bkp_file_name)
        exc = None
        with open(bkp_file_name, 'r+b') as f:
            genesis_bytes = f.read(4)
            if genesis_bytes != self._genesis_bytes:
                exc = exceptions.InvalidDataFileException('Invalid genesis bytes for file')
            else:
                f.seek(snapshot_file_size - len(snapshot_file_last_bytes))
                data = f.read(len(snapshot_file_last_bytes))
                f.seek(0)
                f.truncate(snapshot_file_size)
                if data != snapshot_file_last_bytes:
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

    def _ensure_no_pending_snapshot(self):
        assert not self.running
        if any(map(
            lambda x: x.startswith('.snapshot-'),
            os.listdir(self.path)
        )):
            raise exceptions.InvalidDBStateException('Pending snapshot. DB must be cleaned.')
