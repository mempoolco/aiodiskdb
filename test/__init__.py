import asyncio
import random
import shutil
import time
from unittest import IsolatedAsyncioTestCase

import typing

from aiodiskdb import exceptions
from aiodiskdb.aiodiskdb import AioDiskDB, _TIMEOUT, _FLUSH_INTERVAL
from aiodiskdb.local_types import WriteEvent


class AioDiskDBTestCase(IsolatedAsyncioTestCase):
    _path = '/tmp/aiodiskdb_test'

    def setUp(
            self,
            max_file_size=128,
            max_buffer_size=16,
            overwrite=True,
            genesis_bytes=b'test',
            timeout=_TIMEOUT,
            file_prefix='data',
            flush_interval=_FLUSH_INTERVAL
    ):
        self._flush_interval = flush_interval
        self.loop = asyncio.get_event_loop()
        self._timeout = timeout
        self._overwrite = overwrite
        self._max_file_size = max_file_size
        self._max_buffer_size = max_buffer_size
        self._genesis_bytes = genesis_bytes
        self._file_prefix = file_prefix
        self._writes = []
        self._starts = []
        self._stops = []
        self._index_drops = []
        self._failures = []
        try:
            shutil.rmtree(self._path)
        except FileNotFoundError:
            pass
        self._setup_sut()

    def _hook_events(self):
        self.sut.events.on_start = self._on_start
        self.sut.events.on_stop = self._on_stop
        self.sut.events.on_write = self._on_write
        self.sut.events.on_index_drop = self._on_index_drop
        self.sut.events.on_failure = self._on_failure

    async def _on_write(self, timestamp, event: WriteEvent):
        self._writes.append([timestamp, event])

    async def _on_start(self, timestamp):
        self._starts.append([timestamp])

    async def _on_stop(self, timestamp):
        self._stops.append([timestamp])

    async def _on_index_drop(self, timestamp, index: int, size: int):
        self._index_drops.append([timestamp, index, size])

    async def _on_failure(self, timestamp, exception: typing.Optional[Exception] = None):
        self._failures.append([timestamp, exception])

    def tearDown(self) -> None:
        self.sut.destroy_db()

    def _setup_sut(self, clean_stale_data=True):
        self.sut = AioDiskDB(
            self._path,
            create_if_not_exists=True,
            timeout=self._timeout,
            max_file_size=self._max_file_size,
            max_buffer_size=self._max_buffer_size,
            overwrite=self._overwrite,
            genesis_bytes=self._genesis_bytes,
            clean_stale_data=clean_stale_data,
            file_prefix=self._file_prefix,
            flush_interval=self._flush_interval
        )
        self._hook_events()

    async def _run(self, expect_failure=False):
        async def _handle_run():
            try:
                await self.sut.run()
            except Exception as e:
                if not expect_failure or expect_failure not in str(e):
                    raise

        self.loop.create_task(_handle_run(), name='aiodiskdb_main_loop')
        s = time.time()
        while not self.sut.running:
            await asyncio.sleep(0.01)
            self.assertLess(time.time() - s, 3, msg='timeout')

    async def _stop(self):
        await self.sut.stop()


def run_test_db(f):
    async def _decorator(self, *a, **kw):
        try:
            await self._run()
            return await f(self, *a, **kw)
        finally:
            try:
                await self._stop()
            except exceptions.NotRunningException:
                print('run_test_db requested to shutdown a not running database')
    return _decorator


class AioDiskDBConcurrencyTest(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp()
        self._data = list()
        self._ongoing_reads = False
        self._reads_count = 0
        self._writes_count = 0
        self._transactions = 0
        self._pause_reads = False
        self._stop_reads = False

    async def _random_reads(self):
        self._ongoing_reads = True
        while 1:
            if self._pause_reads:
                self._ongoing_reads = False
                await asyncio.sleep(0.1)
                continue
            else:
                self._ongoing_reads = self._ongoing_reads or True
            if self._stop_reads:
                self._ongoing_reads = False
                break
            try:
                if not self._data:
                    await asyncio.sleep(0.01)
                    continue
                p = random.randint(0, len(self._data))
                location_and_data = self._data[p - 1]
                location, expected_data = location_and_data
                self.assertEqual(
                    expected_data,
                    await self.sut.read(location),
                    msg=location
                )
                self._reads_count += 1
                await asyncio.sleep(0.0001)
            except:
                self._ongoing_reads = False
                raise
        self._ongoing_reads = False
