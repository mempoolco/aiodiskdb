import asyncio
import os
import random
import time

from aiodiskdb import exceptions
from test import AioDiskDBTestCase, run_test_db, AioDiskDBConcurrencyTest


class TestAioDiskDBTransaction(AioDiskDBTestCase):

    @run_test_db
    async def test(self):
        location1 = await self.sut.add(b'data1')
        transaction = await self.sut.transaction()
        transaction.add(b'data2')
        transaction.add(b'data3')
        await transaction.commit()
        with open(self._path + '/data00000.dat', 'rb') as f:
            x = f.read()
        self.assertEqual(x, self.sut._bake_new_file_header() + b'data1data2data3')
        location1.size += 10  # increase the location size to read contiguous data
        self.assertEqual(
            b'data1data2data3',
            await self.sut.read(location1)
        )


class TestAioDiskDBConcurrentTransactions(AioDiskDBConcurrencyTest):
    @run_test_db
    async def test(self):
        self._running_test = True
        self.loop.create_task(self._random_reads())
        data_stored = dict()
        total_size = 0
        s = time.time()
        sizes = []
        while sum(data_stored.values()) < 1000 * 1024 ** 2:
            random_transaction = random.randint(0, 10)
            if random_transaction < 3:
                """
                Add some data by transactions
                """
                transaction = await self.sut.transaction()
                transactions_data = []
                for x in range(0, random.randint(1, 10)):
                    size = random.randint(1024, 1024 ** 2)
                    sizes.append(size)
                    data = os.urandom(size)
                    transactions_data.append(data)
                    transaction.add(data)
                    await asyncio.sleep(0.001)
                locations = await transaction.commit()
                self._writes_count += 1
                self._transactions += 1
                for x in zip(locations, transactions_data):
                    self._data.append(x)  # append [location, data] so the random reads can request it
                    tx_chunk_size = len(x[1])
                    data_stored.setdefault(x[0].index, 0)
                    data_stored[x[0].index] += tx_chunk_size
                    total_size += tx_chunk_size
            else:
                """
                Mix normal adds
                """
                size = random.randint(1024, 1024 ** 2)
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
              f'Reads: {self._reads_count}, Writes: {self._writes_count}, Transactions: {self._transactions}'
              f'Bandwidth: {total_size // 1024 ** 2}MB, '
              f'Avg file size: {sum(sizes) / len(sizes) // 1024}kB'
              )
        current_reads = self._reads_count

        # test is over, repeat the random reads with a new DB instance on the same data.
        print('Read only test with no-cache instance:')
        while self._ongoing_reads:
            await asyncio.sleep(0.2)
        await self.sut.stop()
        self._setup_sut()
        await self._run()
        while not self.sut.running:
            await asyncio.sleep(0.01)
        self._pause_reads = False
        s = time.time()
        while time.time() - s < 10:
            await asyncio.sleep(2)
            self.assertTrue(self._ongoing_reads, msg='reads failed')
        self._stop_reads = True
        print(f'Read only test from disk over. Reads: {self._reads_count - current_reads}')


class TransactionErrors(AioDiskDBTestCase):
    def setUp(self, *a):
        super().setUp(*a, max_file_size=2, max_buffer_size=1)

    @run_test_db
    async def test(self):
        transaction = await self.sut.transaction()
        with self.assertRaises(exceptions.WriteFailedException):
            transaction.add(os.urandom(3*1024**2))
        with self.assertRaises(exceptions.EmptyTransactionException):
            await transaction.commit()
        transaction.add(b'test')
        await transaction.commit()
        with self.assertRaises(exceptions.TransactionAlreadyCommittedException):
            await transaction.commit()
        with self.assertRaises(exceptions.TransactionAlreadyCommittedException):
            await transaction.add(b'bbb')
        transaction = await self.sut.transaction()
        for x in range(0, 100):
            transaction.add(os.urandom(1024**2))
        self.assertTrue(self.sut.running)
        self.loop.create_task(transaction.commit())
        await asyncio.sleep(0.01)
        self.assertTrue(self.sut.running)
        with self.assertRaises(exceptions.TransactionCommitOnGoingException):
            transaction.add(b'aaaa')
