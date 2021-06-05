# aiodiskdb

### Minimal, embeddable on-disk DB, tailored for asyncio.

---
[![Coverage Status](https://coveralls.io/repos/github/mempoolco/aiodiskdb/badge.svg?branch=main)](https://coveralls.io/github/mempoolco/aiodiskdb?branch=main)
 [![PyPI version](https://badge.fury.io/py/aiodiskdb.svg)](https://badge.fury.io/py/aiodiskdb)
[![PyPI license](https://img.shields.io/pypi/l/aiodiskdb.svg)](https://pypi.python.org/pypi/aiodiskdb/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/aiodiskdb.svg)](https://pypi.python.org/pypi/aiodiskdb/)

aiodiskdb is a lightweight, fast, simple append only database.

To be used in the `asyncio` event loop.

### Install

```bash
pip install aiodiskdb
```

### Usage

Start the DB by fire and forget:
```python
from aiodiskdb import AioDiskDB, ItemLocation

db = AioDiskDB('/tmp/aiodiskdb')

loop.create_task(db.start())

```

Use the awaitable API to write and read data.

```python
async def read_and_write():
    new_data_location: ItemLocation = await db.add(b'data')
    data: bytes = await db.read(location)
    assert data == b'data'

    noted_location = ItemLocation(
        index=0,
        position=80,
        length=1024333
    )
    prev_saved_data: bytes = await db.read(noted_location)
    assert len(prev_saved_data) == 1024333
```

Stop the DB before closing the application.
```python
await db.stop()
```

### Asynchronous non blocking

When added, data is saved in RAM and persisted into the disk according customizable settings by a background task, using a ThreadPoolExecutor. 

### Quite enough fast for some use cases

Concurrency tests, part of the unit tests, can be replicated as system benchmark.
The following are performed on a common consumer SSD:
```
Duration: 14.12s,
Reads: 2271 (~162/s),
Writes: 2014 (~143/s),
Bandwidth: 1000MB (71MB/s),
Avg file size: 508.0kB
```

```
Duration: 18.97s,
Reads: 10244 (~540/s),
Writes: 10245 (~540/s),
Bandwidth: 20MB (1.05MB/s),
Avg file size: 1.0kB
```


Inspired by the raw block data storage of the [bitcoincore blocks database](https://en.bitcoin.it/wiki/Bitcoin_Core_0.11_(ch_2):_Data_Storage).

**Still under development, use with care, could become sentient and kill anybody.**