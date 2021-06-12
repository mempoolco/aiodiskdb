import asyncio
from aiodiskdb.local_types import ItemLocation, WriteEvent
from test import AioDiskDBTestCase, run_test_db


class TestFlush(AioDiskDBTestCase):
    @run_test_db
    async def test(self):
        item_location = await self.sut.add(b'test_1')
        self.assertEqual(
            ItemLocation(0, 0, 6),
            item_location
        )
        await self.sut.flush()
        self.assertEqual(self.sut._buffers[-1].data, b'')
        read1 = await self.sut.read(item_location)
        self.assertEqual(b'test_1', read1)


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
        self.assertEqual(self._writes, [])

    def tearDown(self):
        self.assertEqual(1, len(self._stops))
        self.assertIsInstance(self._stops[0][0], float)
        self.assertEqual(1, len(self._starts))
        self.assertIsInstance(self._starts[0][0], float)
        self.assertEqual(1, len(self._writes))
        self.assertIsInstance(self._writes[0][0], float)
        self.assertEqual(
            WriteEvent(index=0, position=0, size=12),
            self._writes[0][1]
        )
        super().tearDown()


class TestReadWriteNonCached(AioDiskDBTestCase):
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
        self.loop.create_task(self.sut.run())
        while not self.sut.running:
            await asyncio.sleep(0.01)
        new_loc_2 = ItemLocation.deserialize(item_location_2.serialize())
        read1 = await self.sut.read(new_loc_2)
        self.assertEqual(b'test_2', read1)
        read2 = await self.sut.read(item_location_2)
        self.assertEqual(b'test_2', read2)

        item_location_3 = item_location_2
        item_location_3.index = 99
        self.assertEqual(None, await self.sut.read(item_location_3))


class TestFlushInterval(AioDiskDBTestCase):
    def setUp(self, *a):
        super().setUp(*a, flush_interval=3)

    @run_test_db
    async def test(self):
        await self.sut.add(b'test_2')
        self.assertEqual(6, self.sut._buffers[-1].size)
        await asyncio.sleep(self.sut._flush_interval + 1)
        self.assertEqual(0, self.sut._buffers[-1].size)
