import os
from aiodiskdb import exceptions
from aiodiskdb.local_types import ItemLocation, WriteEvent
from test import AioDiskDBTestCase, run_test_db


class TestLTRIM(AioDiskDBTestCase):
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
        await self.sut.ltrim(0, 6, safety_check=b'_1')
        item_2 = await self.sut.read(item_location_2)
        self.assertEqual(item_2, b'test_2')
