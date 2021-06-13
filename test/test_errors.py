import asyncio
import os
import time
from pathlib import Path

from aiodiskdb import exceptions
from test import AioDiskDBTestCase


class AioDBTestRunTwiceAndReset(AioDiskDBTestCase):
    async def test(self):
        await self._run()
        self.assertTrue(self.sut.running)
        await self.sut.stop()
        self.assertFalse(self.sut.running)
        s = time.time()
        while not self.sut._stopped:
            if time.time() - s > 3:
                raise ValueError('Unusually slow')
            await asyncio.sleep(0.1)
        self.assertTrue(self.sut._stopped)
        with self.assertRaises(exceptions.InvalidDBStateException):
            await self.sut.run()
        self.assertTrue(self.sut._set_stop)
        self.sut.reset()
        self.assertFalse(self.sut._stopped)
        self.assertFalse(self.sut.running)
        await self._run()
        self.assertTrue(self.sut.running)
        await self.sut.stop()
        s = time.time()
        while not self.sut._stopped:
            if time.time() - s > 3:
                raise ValueError('Unusually slow')
            await asyncio.sleep(0.1)


class AioDBTestErrorWrongFiles(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1, max_buffer_size=1)

    async def test(self):
        await self._run()
        with self.assertRaises(exceptions.EmptyPayloadException):
            await self.sut.add(b'')
        b = os.urandom(1024 ** 2 + 10)
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


class AioDBTestErrorWrongHeaderShort(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp()
        Path(self._path).mkdir(parents=True, exist_ok=True)
        with open(self._path + '/data00000.dat', 'wb') as f:
            f.write(b'aa'*8)

    async def test(self):
        with self.assertRaises(exceptions.InvalidDataFileException):
            await self.sut.run()


class AioDBTestErrorWrongHeader(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp()
        Path(self._path).mkdir(parents=True, exist_ok=True)
        with open(self._path + '/data00000.dat', 'wb') as f:
            f.write(b'aa'*16)

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
        await self._run(expect_failure='data00001.dat should not exists')
        self._corrupt_file()
        with self.assertRaises(exceptions.NotRunningException):
            for _ in range(0, 100):
                await self.sut.add(os.urandom(10240))
        with self.assertRaises(exceptions.NotRunningException) as e:
            await self.sut.run()
        self.assertTrue('ERROR state' in str(e.exception))

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
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(max_file_size=2**32, max_buffer_size=2)


class AioDBTestErrorInvalidGenesisBytes(AioDiskDBTestCase):
    async def test(self):
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(max_file_size=1, max_buffer_size=1, genesis_bytes=b'testtest')


class AioDBTestErrorDBRestartedAfterError(AioDiskDBTestCase):
    async def test(self):
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(max_file_size=1, max_buffer_size=1, genesis_bytes=b'testtest')


class AioDBTestErrorWrongFilesPrefix(AioDiskDBTestCase):
    async def test(self):
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(file_prefix='1222')
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(file_prefix='__aaa')
        with self.assertRaises(exceptions.InvalidConfigurationException):
            super().setUp(file_prefix='')

    def tearDown(self) -> None:
        pass


class AioDBTestStopTimeoutError(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1, max_buffer_size=1, timeout=1)

    async def _fail_flush(self):
        try:
            await self.sut._flush_buffer()
        except exceptions.NotRunningException as e:
            print('Expected exception raised:', str(e))

    async def test(self):
        await self._run(expect_failure='wrong state, cannot recover. buffer lost.')
        await self.sut.add(b'a')
        await self.sut._write_lock.acquire()
        self.loop.create_task(self._fail_flush())
        with self.assertRaises(exceptions.TimeoutException):
            await self.sut.stop()
        self.sut._write_lock.release()

    def tearDown(self) -> None:
        pass


class AioDBTestFileSizeChangedError(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=10, max_buffer_size=2, timeout=1)

    async def _fail_flush(self):
        try:
            await self.sut._flush_buffer()
        except exceptions.InvalidDataFileException as e:
            print('Expected exception raised:', str(e))

    async def test(self):
        await self._run(expect_failure='wrong state, cannot recover. buffer lost.')
        await self.sut.add(os.urandom(2 * 1024**2))
        await self.sut._flush_buffer()
        filename = str(self.sut.path) + '/data00000.dat'

        with open(filename, 'r+b') as f:
            f.seek(os.path.getsize(filename)-10)
            f.truncate()
        await self.sut.add(os.urandom(2 * 1024 ** 2))
        await self._fail_flush()

    def tearDown(self) -> None:
        pass


class AioDBTestMissingFileException(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=10, max_buffer_size=2, timeout=1)

    async def _fail_flush(self):
        try:
            await self.sut._flush_buffer()
        except exceptions.FilesInconsistencyException as e:
            print('Expected exception raised:', str(e))

    async def test(self):
        await self._run(expect_failure='wrong state, cannot recover. buffer lost.')
        await self.sut.add(os.urandom(2 * 1024**2))
        await self.sut._flush_buffer()
        filename = str(self.sut.path) + '/data00000.dat'
        os.remove(filename)
        await self.sut.add(os.urandom(2 * 1024 ** 2))
        await self._fail_flush()

    def tearDown(self) -> None:
        pass
