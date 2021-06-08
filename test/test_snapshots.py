import asyncio
import os
import shutil

from aiodiskdb import exceptions
from test import AioDiskDBTestCase


class TestSnapshots(AioDiskDBTestCase):
    def setUp(
            self, *a,
            max_file_size=16,
            max_buffer_size=1
    ):
        super().setUp(*a, max_file_size=max_file_size, max_buffer_size=max_buffer_size)

    async def test(self):
        await self._run()

        with self.assertRaises(exceptions.InvalidDBStateException):
            await self.sut._clean_db_snapshot(333)

        snapshot_id = 333
        data = os.urandom(1024**2)
        data2 = b'daf'
        item_location = await self.sut.add(data)
        await self.sut._flush_buffer()
        await self.sut._write_db_snapshot(snapshot_id, 0)
        shutil.move(self._path + '/.snapshot-333', self._path + '/_snapshot')
        second_location_add = await self.sut.add(data2)
        assert await self.sut.read(second_location_add) == data2
        await self._stop()
        shutil.move(self._path + '/_snapshot', self._path + '/.snapshot-333')

        with self.assertRaises(exceptions.InvalidDBStateException):
            self._setup_sut(clean_stale_data=False)
        self._setup_sut()

        with self.assertRaises(FileNotFoundError):
            os.path.getsize(self._path + '/.snapshot-333')

        await self._run()
        self.assertIsNone(await self.sut.read(second_location_add))
        self.assertEqual(data, await self.sut.read(item_location))
        await self._stop()
        with open(self._path + '/data00000.dat', 'rb') as f:
            x = f.read()
        self.assertEqual(x, self.sut._bake_new_file_header() + data)

        self._setup_sut()
        await self._run(expect_failure='Requested a snapshot, but a snapshot already exist')
        await self.sut._write_db_snapshot(snapshot_id, 0)
        with self.assertRaises(exceptions.InvalidDBStateException):
            await self.sut._write_db_snapshot(snapshot_id, 0)
        self.assertTrue(self.sut.running)
        with self.assertRaises(exceptions.InvalidDBStateException):
            await self.sut._write_db_snapshot(snapshot_id + 1, 0)
        self.assertTrue(self.sut.running)
        shutil.copy(self._path + '/.snapshot-333', self._path + '/.snapshot-334')
        self.assertTrue(self.sut.running)
        with self.assertRaises(exceptions.NotRunningException) as e:
            for x in range(0, 20):
                await self.sut.add(os.urandom(1024 ** 2))
        self.assertFalse(self.sut.running)
