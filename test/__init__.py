import asyncio
from unittest import IsolatedAsyncioTestCase

from aiodiskdb import exceptions
from aiodiskdb.aiodiskdb import AioDiskDB


class AioDiskDBTestCase(IsolatedAsyncioTestCase):
    _path = '/tmp/aiodiskdb_test'

    def setUp(self, max_file_size=128, max_buffer_size=16, overwrite=True):
        self.loop = asyncio.get_event_loop()
        self._overwrite = True
        self._max_file_size = max_file_size
        self._max_buffer_size = max_buffer_size
        self._setup_sut()
        self.sut.destroy_db()
        self._overwrite = overwrite
        self._setup_sut()

    def tearDown(self) -> None:
        self.sut.destroy_db()

    def _setup_sut(self):
        self.sut = AioDiskDB(
            self._path,
            create_if_not_exists=True,
            read_timeout=5,
            max_file_size=self._max_file_size,
            max_buffer_size=self._max_buffer_size,
            overwrite=self._overwrite
        )


def run_test_db(f):
    async def _decorator(self, *a, **kw):
        try:
            self.loop.create_task(self.sut.run(), name='aiodiskdb_main_loop')
            while not self.sut.running:
                await asyncio.sleep(0.01)
            return await f(self, *a, **kw)
        finally:
            try:
                await self.sut.stop()
            except exceptions.NotRunningException:
                print('run_test_db requested to shutdown a not running database')
    return _decorator
