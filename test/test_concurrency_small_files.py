import asyncio
import os
import time
from random import randint

from test import run_test_db, AioDiskDBConcurrencyTest


class TestAioDiskDBConcurrentReadWriteSmallFiles(AioDiskDBConcurrencyTest):
    @run_test_db
    async def test(self):
        self._running_test = True
        self.loop.create_task(self._random_reads())
        data_stored = dict()
        total_size = 0
        s = time.time()
        sizes = []
        while sum(data_stored.values()) < 20 * 1024 ** 2:
            size = randint(1, 4096)
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
        self._pause_reads = True
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
        self._pause_reads = False
        s = time.time()
        while time.time() - s < 10:
            await asyncio.sleep(2)
            self.assertTrue(self._ongoing_reads, msg='reads failed')
        self._stop_reads = True
        print(f'Read only test from disk over. Reads: {self._reads_count - current_reads}')
