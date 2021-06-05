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
    Minimal on-disk DB, with buffering and timeouts.
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
        self._file_padding = int(file_padding)
        self._max_file_size = int(max_file_size) * 1024 ** 2
        self._max_buffer_items = int(max_buffer_items)
        self._max_buffer_size = int(max_buffer_size) * 1024 ** 2
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
        self._setup_current_buffer()
        self._tmp_idx_and_buffer = TempBufferData(idx=dict(), buffer=None)

    @ensure_async_lock(LockType.WRITE)
    async def _clean_temp_buffer(self):
        self._tmp_idx_and_buffer = TempBufferData(idx=dict(), buffer=None)

    def _read_data_from_buffer(self, location: ItemLocation):
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
        data is placed into the temp buffer while writing it on disk,
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
    def _setup_current_buffer(self):
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

    def enable_overwrite(self):
        self._overwrite = True

    @ensure_async_lock(LockType.WRITE)
    async def _pop_buffer(self) -> None:
        """
        remove the buffer from the data queue.
        put it into the temp storage for disk writing.
        allocate a new buffer for the data queue.
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

    def _save_buffer_to_disk(self):
        try:
            with open(
                self._get_filename_by_idx(self._tmp_idx_and_buffer.buffer.index), 'ab'
            ) as file:
                file.write(self._tmp_idx_and_buffer.buffer.data)
        except FileNotFoundError:
            raise exceptions.NotFoundException(self._tmp_idx_and_buffer.buffer.index)

    async def _flush_buffer(self):
        await self._pop_buffer()
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._save_buffer_to_disk
        )
        await self._clean_temp_buffer()
        self._last_flush = time.time()

    def _process_location_read(self, location: ItemLocation):
        try:
            with open(
                self._get_filename_by_idx(location.index), 'rb'
            ) as file:
                file.seek(location.position)
                return file.read(location.size)
        except FileNotFoundError:
            raise exceptions.DBNotInitializedException

    async def _run_loop(self):
        if self._last_flush is None:
            self._last_flush = time.time()
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

        await self._write_lock.acquire()
        try:
            buffer = self._buffers[-1]
            data_size = len(data)
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
        finally:
            self._write_lock.release()

    @ensure_running(True)
    @ensure_async_lock(LockType.READ)
    async def read(self, location: ItemLocation):
        """
        Reads data from the DB.
        If there's no data in RAM for the given location, try with a disk read.

        :param location: ItemLocation(int, int, int)
        :param timeout: seconds
        :return: bytes
        """
        return self._read_data_from_buffer(location) or \
            self._read_data_from_temp_buffer(location) or \
            await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._process_location_read,
            location
        )

    @ensure_running(False)
    def destroy(self):
        """
        Destroy the DB, clean the disk.
        """
        shutil.rmtree(self.path)
        return True

    @ensure_running(True)
    @ensure_async_lock(LockType.WRITE)
    def blank(self, location: ItemLocation):
        """
        Blank already saved data with zeros.
        """
        ...
