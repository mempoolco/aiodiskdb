import asyncio
from aiodiskdb.types import ItemLocation
from tests import AioDiskDBTestCase, run_test_db


class TestReadWriteCached(AioDiskDBTestCase):
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
        read1 = await self.sut.read(item_location)
        self.assertEqual(b'test_1', read1)
        read2 = await self.sut.read(item_location_2)
        self.assertEqual(b'test_2', read2)


class TestReadWriteNonCached(AioDiskDBTestCase):
    @run_test_db
    async def test(self):
        item_location = await self.sut.add(b'test_1')
        self.assertEqual(
            item_location,
            ItemLocation(0, 0, 6)
        )
        item_location_2 = await self.sut.add(b'test_2')
        self.assertEqual(
            item_location_2,
            ItemLocation(0, 6, 6)
        )
        await self.sut.stop()  # stop the sut, ensures the data write
        self._setup_sut()  # re-instance the sut from scratch.
        self.loop.create_task(self.sut.run())
        while not self.sut.running:
            await asyncio.sleep(0.01)
        read1 = await self.sut.read(item_location_2)
        self.assertEqual(read1, b'test_2')
        read2 = await self.sut.read(item_location_2)
        self.assertEqual(read2, b'test_2')
