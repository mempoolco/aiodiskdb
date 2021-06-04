# aiodiskdb

### Minimal, embeddable on-disk DB, tailored for asyncio.

aiodiskdb is lightweight, fast, simple append only in-file database.

To be used in the `asyncio` event loop.


### Easy to use

Awaitable API for writing and reading data.

```python
import asyncio
from aiodiskdb import AioDiskDB, ItemLocation

db = AioDiskDB('/tmp/aiodiskdb')

async def read_and_write():
    location = await db.add(b'data')
    data = await db.read(location)
```

### Asynchronous non blocking 

Once added, data is saved in RAM, and persisted into the disk according customizable settings. 
Blocking calls are handled by a background tasks.

Start the DB by fire and forget.
```python
self.loop.create_task(db.start())
```

Stop the DB before closing the application.
```python
await db.stop()
```

Inspired by the [bitcoincore blocks database](https://en.bitcoin.it/wiki/Bitcoin_Core_0.11_(ch_2):_Data_Storage),
aiodiskdb store data into files.

As Bitcoincore blocks database, aiodiskdb' files *are about 128 MB, allocated in 16 MB chunks to prevent excessive fragmentation*



