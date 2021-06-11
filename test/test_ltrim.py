import os
from aiodiskdb import exceptions
from aiodiskdb.local_types import ItemLocation, WriteEvent
from test import AioDiskDBTestCase, run_test_db


class TestLTRIM(AioDiskDBTestCase):
    @run_test_db
    async def test(self):
        item_location = await self.sut.add(b'aaaa_1')
        self.assertEqual(
            ItemLocation(0, 0, 6),
            item_location
        )
        item_location_2 = await self.sut.add(b'bbbb_2')
        self.assertEqual(
            ItemLocation(0, 6, 6),
            item_location_2
        )
        item_location_3 = await self.sut.add(b'cccc_3')
        self.assertEqual(
            ItemLocation(0, 12, 6),
            item_location_3
        )
        await self.sut.ltrim(0, 6, safety_check=b'_1')
        item_2 = await self.sut.read(item_location_2)
        self.assertEqual(item_2, b'bbbb_2')
        item_3 = await self.sut.read(item_location_3)
        self.assertEqual(item_3, b'cccc_3')
        with self.assertRaises(exceptions.InvalidTrimCommandException):
            await self.sut.ltrim(0, 6, safety_check=b'_1')
        await self.sut.ltrim(0, 12, safety_check=b'_2')
        item_2 = await self.sut.read(item_location_2)
        self.assertIsNone(item_2)
        item_3 = await self.sut.read(item_location_3)
        self.assertEqual(item_3, b'cccc_3')
