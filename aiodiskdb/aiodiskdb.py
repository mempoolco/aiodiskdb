import asyncio
from collections import OrderedDict
from concurrent.futures.thread import ThreadPoolExecutor
import time
import typing

from aiodiskdb.internals import ensure_running, ensure_future, ensure_async_lock
from aiodiskdb.abstracts import AsyncLockable, AsyncRunnable
from aiodiskdb.types import ItemLocation, Location, LockType

_FILE_SIZE = 128
_FILE_PREFIX = 'DATA'
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
        self.path = path
        self._overwrite = False
        self._file_prefix = file_prefix
        self._file_padding = int(file_padding)
        self._max_file_size = int(max_file_size) * 1024 * 1024
        self._max_buffer_items = int(max_buffer_items)
        self._max_buffer_size = int(max_buffer_size) * 1024 * 1024
        self._current_buffer_size = 0
        self._flush_interval = int(flush_interval)
        if len(self._genesis_bytes) != 4:
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

    def enable_overwrite(self):
        self._overwrite = True

    @property
    def current_buffer_size(self):
        return self._current_buffer_size

    async def _read_location(self, location: ItemLocation) -> asyncio.Future:
        """
        Reads are achieved by evading the reads queue.
        Multiple reads on the same file are batched.
        With a single file open multiple reads are evaded.
        """
        future = asyncio.Future()
        self._pending_reads_by_idx.setdefault(location.index, set())
        self._pending_reads_by_idx[location.index].add([location, future])
        return future

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
            position=int(self._current_add_location.position),
            size_bytes=int(self._current_add_location.size_bytes)
        )
        return buffer, buffer_size, add_location

    async def _save_buffer_to_disk(self, buffer: typing.List[bytes], size: int, location: Location):
        ...

    async def _flush_buffer(self):
        await self._write_lock.acquire()
        buffer, buffer_size, add_location = self._pop_buffer()
        self._write_lock.release()
        await self._save_buffer_to_disk(buffer, buffer_size, add_location)

    def _process_reads_for_index(self, index: int):
        """
        Reads are sorted so that the read is contiguous.
        """
        locations = sorted(
            self._pending_reads_by_idx.pop(index),
            key=lambda x: x[0].position
        )
        with open(f'{self.path}/{self._file_prefix}_' + f'{index}'.zfill(self._file_padding)) as file:
            for location in locations:
                file.seek(location[0].position)
                data = file.read(location[0].length)
                location[1].set_result(data)

    async def _process_pending_reads(self):
        """
        Pending reads are fired as async tasks.
        """
        futures = map(
            lambda index: asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._process_reads_for_index,
                index
            ),
            list(self._pending_reads_by_idx.keys())
        )
        return await asyncio.gather(*futures)

    async def _run_loop(self):
        if self._pending_reads_by_idx:
            await self._process_pending_reads()

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
            return location
        finally:
            self._write_lock.release()

    @ensure_running(True)
    @ensure_async_lock(LockType.READ)
    @ensure_future
    async def read(self, location: ItemLocation, timeout=None):
        """
        Reads data from the storage.
        If there's no data in RAM for the given location, try with a disk read.

        :param location: ItemLocation(int, int, int)
        :param timeout: seconds
        :return: bytes
        """
        res = self._buffer_index.get(location.serialized)
        if res:
            return res
        future = self._read_location(location)
        return await asyncio.wait_for(
            future,
            timeout=timeout is not None and int(timeout) or self._read_timeout
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
        pass
