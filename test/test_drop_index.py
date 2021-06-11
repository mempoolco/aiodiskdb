from aiodiskdb import exceptions
from test import AioDiskDBTestCase, run_test_db


class AioDBTestDropIndex(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1, max_buffer_size=1, overwrite=False)

    async def test(self):
        await self._run()
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
        with self.assertRaises(exceptions.IndexDoesNotExist):
            await self.sut.drop_index(99)
        transaction = await self.sut.transaction()
        transaction.add(b'cafe')
        t_loc = await transaction.commit()
        self.assertTrue(bool(self.sut._buffers))
        loc_0 = await self.sut.add(b'babe')
        self.assertEqual(b'babe', await self.sut.read(loc_0))
        self.assertEqual(b'cafe', await self.sut.read(t_loc[0]))
        await self.sut.drop_index(0)
        self.assertIsNone(await self.sut.read(loc_0))
        self.assertIsNone(await self.sut.read(t_loc[0]))
        loc = await self.sut.add(b'test_after')
        d = await self.sut.read(loc)
        self.assertEqual(d, b'test_after')
        await self._stop()
        self._setup_sut()
        self.sut.enable_overwrite()
        await self._run()
        await self.sut.drop_index(0)
        await self._stop()

    def tearDown(self) -> None:
        self.assertEqual(2, len(self._index_drops))
        self.assertIsInstance(self._index_drops[0][0], float)
        self.assertEqual(self._index_drops[0][1], 0)  # index
        self.assertEqual(self._index_drops[0][2], 4)  # length of 'cafe'
        super().tearDown()
