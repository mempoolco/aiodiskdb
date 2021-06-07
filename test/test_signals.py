import asyncio
import os

from test import AioDiskDBTestCase, run_test_db


class AioDBTestExitSignals(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1, max_buffer_size=1)
        self._added_location = None

    async def _persist_data(self):
        while not self.sut.running:
            await asyncio.sleep(1)
        self._added_location = await self.sut.add(b'data')

    @run_test_db
    async def test(self):
        """
        test that the stop signals hook writes data before exiting
        """
        await self._persist_data()
        self.assertIsNotNone(self._added_location)
        with self.assertRaises(FileNotFoundError):
            os.path.getsize(self._path + '/data00000.dat')
        with self.assertRaises(SystemExit):
            self.sut.on_stop_signal()
        self.sut.on_stop_signal()  # fire it twice, it must be executed once
        self.assertEqual(
            self.sut._file_header_size + 4,
            os.path.getsize(self._path + '/data00000.dat')
        )
        self._setup_sut()
        asyncio.get_event_loop().create_task(self.sut.run())
        while not self.sut.running:
            await asyncio.sleep(1)
        data = await self.sut.read(self._added_location)
        self.assertEqual(b'data', data)
