import os
import time
from pathlib import Path

from aiodiskdb import exceptions
from test import AioDiskDBTestCase


class AioDBTestErrorWrongFiles(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1, max_buffer_size=1)

    async def test(self):
        await self._run()
        b = os.urandom(1024 ** 2 + 1)
        with self.assertRaises(exceptions.WriteFailedException):
            await self.sut.add(b)
        await self._stop()

    def tearDown(self) -> None:
        self.assertEqual(1, len(self._stops))
        self.assertIsInstance(self._stops[0][0], float)
        self.assertEqual(1, len(self._starts))
        self.assertIsInstance(self._starts[0][0], float)
        self.assertEqual(0, len(self._failures))
        self.assertEqual(0, len(self._writes))
        super().tearDown()


class AioDBTestErrorWrongGenesis(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp()
        Path(self._path).mkdir(parents=True, exist_ok=True)
        with open(self._path + '/data00000.dat', 'wb') as f:
            f.write(b'aa'*8)

    async def test(self):
        with self.assertRaises(exceptions.InvalidDataFileException):
            await self.sut.run()


class AioDBTestErrorWrongGenesisFileShouldNotExists(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=0.1, max_buffer_size=0.1)

    def _corrupt_file(self):
        Path(self._path).mkdir(parents=True, exist_ok=True)
        with open(self._path + '/data00001.dat', 'wb') as f:
            f.write(b'aa'*8)

    async def test(self):
        await self._run(expect_failure=True)
        self._corrupt_file()
        with self.assertRaises(exceptions.NotRunningException):
            for _ in range(0, 100):
                await self.sut.add(os.urandom(10240))
        self.assertTrue('File /tmp/aiodiskdb_test/data00001.dat should not exists' in str(self.sut._error))

    def tearDown(self):
        self.assertEqual(1, len(self._stops))
        self.assertIsInstance(self._stops[0][0], float)
        self.assertEqual(1, len(self._starts))
        self.assertIsInstance(self._starts[0][0], float)
        self.assertEqual(1, len(self._failures))
        self.assertIsInstance(self._failures[0][0], float)
        self.assertTrue(self._failures[0][0] - time.time() < 2)
        super().tearDown()


class AioDBTestErrorZeroDBSizeError(AioDiskDBTestCase):
    async def test(self):
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(max_file_size=0, max_buffer_size=0)


class AioDBTestErrorInvalidDBSizeError(AioDiskDBTestCase):
    async def test(self):
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(max_file_size=1, max_buffer_size=2)


class AioDBTestErrorInvalidGenesisBytes(AioDiskDBTestCase):
    async def test(self):
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(max_file_size=1, max_buffer_size=1, genesis_bytes=b'testtest')
