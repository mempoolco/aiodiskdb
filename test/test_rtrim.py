import os
from aiodiskdb import exceptions
from aiodiskdb.local_types import ItemLocation, WriteEvent
from test import AioDiskDBTestCase, run_test_db


class TestRTRIM(AioDiskDBTestCase):
    @run_test_db
    async def test(self):
        item_location = await self.sut.add(b'test_1')
        self.assertEqual(
            ItemLocation(0, 0, 6),
            item_location
        )
        item_location_2 = await self.sut.add(b'test_2')
        self.assertEqual(
            ItemLocation(0, 6, 6),
            item_location_2
        )
        await self.sut.stop()  # stop the sut, ensures the data write
        self.assertEqual(
            WriteEvent(index=0, position=0, size=12),
            self._writes[0][1]
        )
        self._setup_sut()  # re-instance the sut from scratch.
        await self._run()
        read1 = await self.sut.read(item_location_2)
        self.assertEqual(b'test_2', read1)
        read2 = await self.sut.read(item_location_2)
        self.assertEqual(b'test_2', read2)
        slice_size = await self.sut.rtrim(0, 9, safety_check=b't_2')
        slice_size_2 = await self.sut.rtrim(0, 6)
        self.assertEqual(slice_size + slice_size_2, len(read2))
        self.assertIsNone(await self.sut.read(item_location_2))
        item_location_2 = await self.sut.add(b'test_3')
        self.assertEqual(
            ItemLocation(0, 6, 6),
            item_location_2
        )
        read3 = await self.sut.read(item_location_2)
        self.assertEqual(b'test_3', read3)
        self.sut.disable_overwrite()
        with self.assertRaises(exceptions.ReadOnlyDatabaseException):
            await self.sut.rtrim(0, 6)
        self.sut.enable_overwrite()


class TestTrimIndexDoesNotExist(AioDiskDBTestCase):
    @run_test_db
    async def test(self):
        with self.assertRaises(exceptions.IndexDoesNotExist):
            await self.sut.rtrim(99, 111)

        with self.assertRaises(exceptions.IndexDoesNotExist):
            await self.sut.rtrim(-1, 111)


class TestTrimWholeFile(AioDiskDBTestCase):
    @run_test_db
    async def test(self):
        for _ in range(0, 20):
            await self.sut.add(os.urandom(1024**2))
        await self.sut.rtrim(0, 0)
        with self.assertRaises(FileNotFoundError):
            os.path.getsize(self._path + '/data00000.dat')
        for _ in range(0, 20):
            await self.sut.add(os.urandom(1024**2))
        with self.assertRaises(exceptions.InvalidTrimCommandException):
            await self.sut.rtrim(0, 0, safety_check=b'wrong_bytes')
        with self.assertRaises(exceptions.InvalidTrimCommandException):
            await self.sut.rtrim(0, 1, safety_check=b'wrong_bytes')
