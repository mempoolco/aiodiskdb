import asyncio
from unittest import IsolatedAsyncioTestCase

from aiodiskdb.aiodiskdb import AioDiskDB


class AioDiskDBTestCase(IsolatedAsyncioTestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self._max_file_size = 128
        self._setup_sut()
        self.sut.destroy()
        self._setup_sut()

    def tearDown(self) -> None:
        self.sut.destroy()

    def _setup_sut(self):
        self.sut = AioDiskDB(
            '/tmp/aiodiskdb_tests',
            create_if_not_exists=True,
            read_timeout=5,
            max_file_size=self._max_file_size
        )


def run_test_db(f):
    async def _decorator(self, *a, **kw):
        try:
            self.loop.create_task(self.sut.run())
            while not self.sut.running:
                await asyncio.sleep(0.01)
            return await f(self, *a, **kw)
        finally:
            await self.sut.stop()
    return _decorator
