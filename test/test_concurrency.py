import asyncio
import os
import random
import time
from random import randint

from test import AioDiskDBTestCase, run_test_db


class TestAioDiskDBConcurrentReadWrite(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp()
        self._data = list()
        self._running_test = False
        self._ongoing_reads = False
        self._reads_count = 0
        self._writes_count = 0

    async def _random_reads(self):
        self._ongoing_reads = True
        while self._running_test:
            try:
                if not self._data:
                    await asyncio.sleep(0.01)
                    continue
                p = random.randint(0, len(self._data))
                location_and_data = self._data[p - 1]
                location, expected_data = location_and_data
                self.assertEqual(
                    expected_data,
                    await self.sut.read(location),
                    msg=location
                )
                self._reads_count += 1
                await asyncio.sleep(0.0001)
            except:
                self._ongoing_reads = False
                raise
        self._ongoing_reads = False

    @run_test_db
    async def test(self):
        self._running_test = True
        self.loop.create_task(self._random_reads())
        data_stored = dict()
        total_size = 0
        s = time.time()
        sizes = []
        while sum(data_stored.values()) < 1000 * 1024 ** 2:
            size = randint(1024, 1024**2)
            sizes.append(size)
            data = os.urandom(size)
            location = await self.sut.add(data)
            self._writes_count += 1
            data_stored.setdefault(location.index, 0)
            self._data.append([location, data])
            data_stored[location.index] += size
            total_size += size
            await asyncio.sleep(0.00001)
            self.assertTrue(self._ongoing_reads, msg='reads failed')
        self._running_test = False
        print(f'R/W concurrency test over. Duration: {time.time() - s:.2f}s, '
              f'Reads: {self._reads_count}, Writes: {self._writes_count}, '
              f'Bandwidth: {total_size // 1024 ** 2}MB, '
              f'Avg file size: {sum(sizes) / len(sizes) // 1024}kB'
              )

        current_reads = self._reads_count
        # test is over, repeat the random reads with a new DB instance on the same data.
        print('Read only test with no-cache instance:')
        while self._ongoing_reads:
            await asyncio.sleep(1)
        await self.sut.stop()
        self._setup_sut()
        self.loop.create_task(self.sut.run())
        while not self.sut.running:
            await asyncio.sleep(0.01)
        self._running_test = True
        self.loop.create_task(self._random_reads())
        s = time.time()
        while time.time() - s < 10:
            await asyncio.sleep(2)
            self.assertTrue(self._ongoing_reads, msg='reads failed')
        self._running_test = False
        print(f'Read only test from disk over. Reads: {self._reads_count - current_reads}')

