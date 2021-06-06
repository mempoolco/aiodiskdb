from test import AioDiskDBTestCase, run_test_db


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
        self.assertEqual(x, self.sut._genesis_bytes + b'data1data2data3')
        location1.size += 10
        self.assertEqual(
            b'data1data2data3',
            await self.sut.read(location1)
        )
