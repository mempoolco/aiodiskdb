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
from aiodiskdb.types import ItemLocation, Location, LockType

_FILE_SIZE = 128
_FILE_PREFIX = 'data'
_FILE_ZEROS_PADDING = 5
_BUFFER_SIZE = 16
_BUFFER_ITEMS = 2000
_FLUSH_INTERVAL = 300
_GENESIS_BYTES = b'\r\xce\x8f7'
_READ_TIMEOUT = 30
_CONCURRENCY = 32


class AioDiskDB(AsyncLockable, AsyncRunnable):
    """
    Minimal append only on-disk DB, with buffering and timeouts.
    Made with love for Asyncio.
    """

    def __init__(
            self,
            path: str,
            start_location: Location = Location(0, 0, 0),
            create_if_not_exists: bool = False,
            overwrite: bool = False,
            file_padding: int = _FILE_ZEROS_PADDING,
            file_prefix: str = _FILE_PREFIX,
            max_file_size: int = _FILE_SIZE,
            max_buffer_items: int = _BUFFER_ITEMS,
            max_buffer_size: int = _BUFFER_SIZE,
            flush_interval: int = _FLUSH_INTERVAL,
            genesis_bytes: bytes = _GENESIS_BYTES,
            read_timeout: int = _READ_TIMEOUT,
            concurrency: int = _CONCURRENCY
    ):
        super().__init__()
        self.path = Path(path)
        if create_if_not_exists:
            self.path.mkdir(parents=True, exist_ok=True)
        self._overwrite = False
        self._file_prefix = file_prefix
        self._file_padding = int(file_padding)
        self._max_file_size = int(max_file_size) * 1024 * 1024
        self._max_buffer_items = int(max_buffer_items)
        self._max_buffer_size = int(max_buffer_size) * 1024 * 1024
        self._current_buffer_size = 0
        self._flush_interval = int(flush_interval)
        if len(genesis_bytes) != 4:
            raise ValueError('Genesis bytes length must be 4')
        self._genesis_bytes = genesis_bytes
        self._read_timeout = int(read_timeout)
        self._data_queue = asyncio.Queue()
        self._current_write_file = None
        self._buffer_index = OrderedDict()
        self._buffer = list()
        self._read_files = OrderedDict()
        self._last_flush = None
        self._pending_reads_by_idx = OrderedDict()
        self._concurrency = int(concurrency)
        self.executor = ThreadPoolExecutor(max_workers=concurrency)
        self._current_add_location = start_location
        self._overwrite = overwrite

    async def _teardown(self):
        if self.current_buffer_size:
            await self._flush_buffer()

    def enable_overwrite(self):
        self._overwrite = True

    @property
    def current_buffer_size(self):
        return self._current_buffer_size

    def _pop_buffer(self) -> typing.Tuple[typing.List, int, Location]:
        buffer, buffer_size = self._buffer, int(self._current_buffer_size)
        self._buffer, self._buffer_index = [], {}
        if self._current_buffer_size > self._max_buffer_size:
            self._current_add_location = Location(
                index=self._current_add_location.index + 1,
                position=0, size_bytes=0
            )
        self._current_buffer_size = 0
        add_location = Location(
            index=int(self._current_add_location.index),
            position=int(self._current_add_location.position) - buffer_size,
            size_bytes=int(self._current_add_location.size_bytes)
        )
        return buffer, buffer_size, add_location

    async def _save_buffer_to_disk(self, buffer: typing.List[bytes], location: Location):
        try:
            with open(
                f'{self.path}/{self._file_prefix}' + f'{location.index}'.zfill(self._file_padding) + '.dat',
                'wb'
            ) as file:
                file.seek(location.position)
                for b in buffer:
                    data = file.write(b)
                return data
        except FileNotFoundError:
            raise exceptions.NotFoundException(location)

    async def _flush_buffer(self):
        await self._write_lock.acquire()
        buffer, buffer_size, add_location = self._pop_buffer()
        self._write_lock.release()
        await self._save_buffer_to_disk(buffer, add_location)

    def _process_location_read(self, location: ItemLocation):
        try:
            with open(
                f'{self.path}/{self._file_prefix}' + f'{location.index}'.zfill(self._file_padding) + '.dat',
                'rb'
            ) as file:
                file.seek(location.position)
                data = file.read(location.length)
                return data
        except FileNotFoundError:
            raise exceptions.DBNotInitializedException

    async def _run_loop(self):
        if self._last_flush is None:
            self._last_flush = time.time()

        if self.current_buffer_size > self._max_buffer_size:
            await self._flush_buffer()
        elif time.time() - self._last_flush > self._flush_interval:
            await self._flush_buffer()
        elif len(self._buffer) > self._max_buffer_items:
            await self._flush_buffer()

    @ensure_running(True)
    async def add(self, data: bytes) -> ItemLocation:
        """
        Put data into the buffer, update the index for reads from RAM.

        :param data: bytes
        :return: ItemLocation(int, int, int)
        """
        while self._current_buffer_size > self._max_buffer_size:
            await asyncio.sleep(0.01)

        await self._write_lock.acquire()
        try:
            data_size = len(data)
            location = ItemLocation(
                self._current_add_location.index,
                self._current_add_location.position,
                data_size
            )
            self._buffer.append(data)
            self._buffer_index[location.serialized] = len(self._buffer) - 1
            self._current_add_location.position += data_size
            self._current_add_location.size_bytes += data_size
            self._current_buffer_size += data_size
            return location
        finally:
            self._write_lock.release()

    @ensure_running(True)
    @ensure_async_lock(LockType.READ)
    async def read(self, location: ItemLocation):
        """
        Reads data from the storage.
        If there's no data in RAM for the given location, try with a disk read.

        :param location: ItemLocation(int, int, int)
        :param timeout: seconds
        :return: bytes
        """
        idx = self._buffer_index.get(location.serialized)
        if idx is not None:
            return self._buffer[idx]
        return await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self._process_location_read,
            location
        )

    @ensure_running(True)
    @ensure_async_lock(LockType.WRITE)
    async def pop(self, location: ItemLocation):
        """
        Append only database, data can only be removed from some point to the end of file.
        Limit must be None since all the data from the specified position is removed.

        :param location: ItemLocation(int, int, None)
        return: bytes
        """
        pass

    @ensure_running(False)
    def destroy(self):
        """
        Destroy the DB, clean the disk.
        """
        shutil.rmtree(self.path)
        return True
