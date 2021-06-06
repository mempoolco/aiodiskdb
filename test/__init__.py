import asyncio
from unittest import IsolatedAsyncioTestCase

import typing

from aiodiskdb import exceptions
from aiodiskdb.aiodiskdb import AioDiskDB
from aiodiskdb.local_types import WriteEvent


class AioDiskDBTestCase(IsolatedAsyncioTestCase):
    _path = '/tmp/aiodiskdb_test'

    def setUp(self, max_file_size=128, max_buffer_size=16, overwrite=True, genesis_bytes=b'test'):
        self.loop = asyncio.get_event_loop()
        self._overwrite = True
        self._max_file_size = max_file_size
        self._max_buffer_size = max_buffer_size
        self._genesis_bytes = genesis_bytes
        self._writes = []
        self._starts = []
        self._stops = []
        self._index_drops = []
        self._failures = []
        self._setup_sut()
        self.sut.destroy_db()
        self._overwrite = overwrite
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

    def _setup_sut(self):
        self.sut = AioDiskDB(
            self._path,
            create_if_not_exists=True,
            read_timeout=5,
            max_file_size=self._max_file_size,
            max_buffer_size=self._max_buffer_size,
            overwrite=self._overwrite,
            genesis_bytes=self._genesis_bytes
        )
        self._hook_events()

    async def _run(self, expect_failure=False):
        async def _handle_run():
            try:
                await self.sut.run()
            except:
                if not expect_failure:
                    raise

        self.loop.create_task(_handle_run(), name='aiodiskdb_main_loop')
        while not self.sut.running:
            await asyncio.sleep(0.01)

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
