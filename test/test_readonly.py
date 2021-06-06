from aiodiskdb import exceptions
from test import AioDiskDBTestCase, run_test_db


class AioDBTestErrorWrongFiles(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1, max_buffer_size=1, overwrite=False)

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
        with self.assertRaises(exceptions.IndexDoesNotExist):
            await self.sut.drop_index(99)
        transaction = await self.sut.transaction()
        transaction.add(b'cafe')
        await transaction.commit()
        await self.sut.drop_index(0)

    def tearDown(self) -> None:
        self.assertEqual(1, len(self._index_drops))
        self.assertIsInstance(self._index_drops[0][0], float)
        self.assertEqual(self._index_drops[0][1], 0)  # index
        self.assertEqual(self._index_drops[0][2], 4)  # length of 'cafe'
        print(self._index_drops)
        super().tearDown()
