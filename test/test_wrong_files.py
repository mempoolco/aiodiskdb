import os

from aiodiskdb import exceptions
from test import AioDiskDBTestCase, run_test_db


class AioDBTestErrorWrongFiles(AioDiskDBTestCase):
    def setUp(self, *a, **kw):
        super().setUp(max_file_size=1)

    @run_test_db
    async def test(self):
        b = os.urandom(1024 ** 2 + 1)
        with self.assertRaises(exceptions.WriteFailedException):
            await self.sut.add(b)
