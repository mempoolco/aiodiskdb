import asyncio
import time
from unittest import IsolatedAsyncioTestCase

from aiodiskdb.aiodiskdb import AioDiskDB


class TestAioDiskDB(IsolatedAsyncioTestCase):
    def setUp(self):
        self._setup_sut()
        self.loop = asyncio.get_event_loop()

    def _setup_sut(self):
        self.sut = AioDiskDB(
            '/tmp/aiodiskdb',
            create_if_not_exists=True,
            read_timeout=5
        )

    async def test_basic_read_write(self):
        self.loop.create_task(self.sut.run())
        await asyncio.sleep(0.1)
        item_location = await self.sut.add(b'test_1')
        read1 = await self.sut.read(item_location)
        self.assertEqual(read1, b'test_1')
        await self.sut.stop()
        self._setup_sut()
        self.loop.create_task(self.sut.run())
        await asyncio.sleep(0.1)
        s = time.time()
        read2 = await self.sut.read(item_location)
        print('{:.8f}'.format(time.time() - s))
        self.assertEqual(read2, b'test_1')
        await self.sut.stop()
