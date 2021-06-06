import asyncio
import os

from aiodiskdb import exceptions
from test import AioDiskDBTestCase, run_test_db


class AioDBTestErrorWrongFiles(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1, max_buffer_size=1, overwrite=False)

    async def _run(self):
        asyncio.get_event_loop().create_task(self.sut.run())
        while not self.sut.running:
            await asyncio.sleep(0.1)

    @run_test_db
    async def test(self):
        """
        test that the stop signals hook writes data before exiting
        """
        with self.assertRaises(exceptions.ReadOnlyDatabaseException):
            await self.sut.drop_index(99)
        with self.assertRaises(exceptions.RunningException):
            self.sut.destroy_db()
        await self.sut.stop()
        with self.assertRaises(exceptions.ReadOnlyDatabaseException):
            self.sut.destroy_db()
        self._setup_sut()
        await self._run()
        self.sut.enable_overwrite()
        await self.sut.drop_index(99)

    def tearDown(self) -> None:
        self.assertEqual(1, len(self._index_drops))
        self.assertIsInstance(self._index_drops[0][0], float)
        print(self._index_drops)
        super().tearDown()
