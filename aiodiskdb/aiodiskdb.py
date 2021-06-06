import asyncio
import os
import shutil
from collections import OrderedDict
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import time
import typing

from aiodiskdb import exceptions
from aiodiskdb.internals import ensure_running,  ensure_async_lock
from aiodiskdb.abstracts import AsyncLockable, AsyncRunnable
from aiodiskdb.types import ItemLocation, LockType, Buffer, TempBufferData

_FILE_SIZE = 128
_FILE_PREFIX = 'data'
_FILE_ZEROS_PADDING = 5
_BUFFER_SIZE = 16
_BUFFER_ITEMS = 1000
_FLUSH_INTERVAL = 30
_GENESIS_BYTES = b'\r\xce\x8f7'
_TIMEOUT = 30
_CONCURRENCY = 32


class AioDiskDB(AsyncLockable, AsyncRunnable):
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
            file_padding: int = _FILE_ZEROS_PADDING,
            file_prefix: str = _FILE_PREFIX,
            max_file_size: int = _FILE_SIZE,
            max_buffer_items: int = _BUFFER_ITEMS,
            max_buffer_size: int = _BUFFER_SIZE,
            flush_interval: int = _FLUSH_INTERVAL,
            genesis_bytes: bytes = _GENESIS_BYTES,
            read_timeout: int = _TIMEOUT,
            write_timeout: int = _TIMEOUT,
            concurrency: int = _CONCURRENCY
    ):
        super().__init__()
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
            raise ValueError('Genesis bytes length must be 4')
        self._genesis_bytes = genesis_bytes
        self._read_timeout = int(read_timeout)
        self._write_timeout = int(write_timeout)
        self._buffer_index = OrderedDict()
        self._buffers: typing.List[Buffer] = list()
        self._last_flush = None
        self._executor = ThreadPoolExecutor(max_workers=concurrency)
        self._overwrite = overwrite
        self._tmp_idx_and_buffer = TempBufferData(idx=dict(), buffer=None)

    def on_stop_signal(self):
        """
        Handle graceful stop signals. Flush buffer to disk.
        """
        if self._blocking_stop:
            return
        self._blocking_stop = True
        if self._tmp_idx_and_buffer.idx:
            self._save_buffer_to_disk(self._tmp_idx_and_buffer)
        self._tmp_idx_and_buffer = None
        while self._buffer_index:
            buffer = self._buffers.pop(0)
            v = self._buffer_index.pop(buffer.index)
            self._save_buffer_to_disk(TempBufferData(idx={buffer.index: v}, buffer=buffer))

    @ensure_async_lock(LockType.WRITE)
    async def _clean_temp_buffer(self):
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
        Data is placed into the temp buffer while writing it on disk,
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

    def _get_filename_by_idx(self, idx: int) -> str:
        return f'{self.path}/{self._file_prefix}' + f'{idx}'.zfill(self._file_padding) + '.dat'

    @ensure_running(False)
    async def _setup_current_buffer(self):
        """
        Setup the current buffer, starting from the disk files.
        If no files are found, setup a fresh buffer, otherwise check the genesis bytes.
        """
        files = sorted(os.listdir(self.path))
        last_file = files and files[-1]
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

    @ensure_async_lock(LockType.WRITE)
    async def _pop_buffer(self) -> TempBufferData:
        """
        Remove the buffer from the data queue.
        Put it into the temp storage for disk writing.
        Allocate a new buffer for the data queue.
        """
        assert not self._tmp_idx_and_buffer.buffer
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

    async def _flush_buffer(self):
        """
        Trigger blocking\non-blocking operations.
        - pop_buffer: coroutine, locked for writing
        - save_buffer_to_disk: blocking threaded task, non locked
        - clean_temp_buffer: coroutine, locked for writing
        """
        buffer = await self._pop_buffer()
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._save_buffer_to_disk,
            buffer
        )
        await self._clean_temp_buffer()
        self._last_flush = time.time()

    def _process_location_read(self, location: ItemLocation):
        """
        Actually read data from files.
        """
        try:
            with open(
                self._get_filename_by_idx(location.index), 'rb'
            ) as file:
                file.seek(location.position)
                return file.read(location.size)
        except FileNotFoundError:
            return b''

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
        s = time.time()
        while self._buffers[-1].file_size >= self._max_file_size \
                or self._buffers[-1].size >= self._max_buffer_size:
            # wait for the LRT to shift the buffers
            await asyncio.sleep(0.01)
            if time.time() - s > self._write_timeout:
                raise exceptions.WriteTimeoutException
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

    @ensure_async_lock(LockType.WRITE)
    async def add_many(self, data: bytes) -> typing.Sequence[ItemLocation]:
        """
        Add multiple entries.
        Return multiple locations.
        Block all the access to the DB, but grant atomic "all or nothing" write, even on multiple files.
        """
        ...

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
    async def drop_index(self, index: int):
        """
        Destroy a single index.
        """
        if not self._overwrite:
            raise exceptions.ReadOnlyDatabaseException
