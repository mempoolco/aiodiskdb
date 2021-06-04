# aiodiskdb

### Minimal, embeddable on-disk DB, tailored for asyncio.

aiodiskdb is lightweight, fast, simple append only in-file database.

the codebase is tailored for asyncio.


###Easy to use
```python
import asyncio
from aiodiskdb import AioDiskDB, ItemLocation

db = AioDiskDB('/tmp/aiodiskdb')

async def read_and_write():
    location = await db.add(b'data')
    assert b'data' == await db.read(location)

```

The database works with *asyncio* event loops, and must be started and stopped before and after the usage:

```python
self.loop.create_task(db.start())  # lrt, must be fired as task
await db.stop()  # must be awaited
```


Inspired by the [bitcoincore blocks database](https://en.bitcoin.it/wiki/Bitcoin_Core_0.11_(ch_2):_Data_Storage),
aiodiskdb store data into files.

As Bitcoincore blocks database, aiodiskdb' files *are about 128 MB, allocated in 16 MB chunks to prevent excessive fragmentation*


