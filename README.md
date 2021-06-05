![aiodiskdb logo](./docs/logo128.png "aiodiskdb")
### Minimal, embeddable on-disk DB, tailored for asyncio.

---
[![Coverage Status](https://coveralls.io/repos/github/mempoolco/aiodiskdb/badge.svg?branch=main)](https://coveralls.io/github/mempoolco/aiodiskdb?branch=main)
[![PyPI version](https://badge.fury.io/py/aiodiskdb.svg)](https://badge.fury.io/py/aiodiskdb)
[![PyPI license](https://img.shields.io/pypi/l/aiodiskdb.svg)](https://pypi.python.org/pypi/aiodiskdb/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/aiodiskdb.svg)](https://pypi.python.org/pypi/aiodiskdb/)
[![Build Status](https://travis-ci.com/mempoolco/aiodiskdb.svg?branch=main)](https://travis-ci.com/mempoolco/aiodiskdb)
[![Donate with Bitcoin](https://en.cryptobadges.io/badge/micro/3FVGopUDc6tyAP6t4P8f3GkYTJ5JD5tPwV)](https://en.cryptobadges.io/donate/3FVGopUDc6tyAP6t4P8f3GkYTJ5JD5tPwV)

**aiodiskdb** is a lightweight, fast, simple **append only** database.

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

Use the db API to write and read data from a coroutine.

```python
async def read_and_write():
    new_data_location: ItemLocation = await db.add(b'data')
    data: bytes = await db.read(location)
    assert data == b'data'

    noted_location = ItemLocation(
        index=0,
        position=80,
        size=1024333
    )
    prev_saved_data: bytes = await db.read(noted_location)
    assert len(prev_saved_data) == 1024333
```

Stop the DB before closing the application.
```python
await db.stop()
```

Be alerted when data is actually persisted to disk:

```python
async def callback(location: ItemLocation):
    log(f'{location} persisted to disk.')
    await do_something(location)
    
db.on_write = callback
```

Or hook to many other events:
```python
db.on_start = ...
db.on_stop = ...
db.on_failure = ...
db.on_destroy_db = ...
db.on_destroy_index = ...
```

### Asynchronous non-blocking

Handle file writes with no locks. 
Data is appended in RAM and persisted asynchronously, according to customizable settings. 

### Transactional

"All or nothing" commit. 
Lock all the DB write operations while in transaction, allow the reads.
Ensure an arbitrary sequence of data is persisted to disk.

Transaction is scoped. Data added into a transaction is not available outside until committed.
```python
transaction = await db.transaction()

transaction.add(b'cafe')
transaction.add(b'babe')
transaction.add(b'deadbeef')

locations: typing.Sequence[ItemLocation] = await transaction.commit()
```

### Highly customizable

The default parameters: 
```python
_FILE_SIZE = 128
_FILE_PREFIX = 'data'
_FILE_ZEROS_PADDING = 5
_BUFFER_SIZE = 16
_BUFFER_ITEMS = 1000
_FLUSH_INTERVAL = 30
_TIMEOUT = 30
_CONCURRENCY = 32
```
can be easily customized. In the following example the files max size is 16 MB,
and data is persisted to disk every 1 MB **OR** every 100 new items **OR** every minute.

```python
db = AioDiskDB(
    max_file_size=16
    max_buffer_size=1,
    max_buffer_items=100,
    flush_interval=60
)
```
The max DB size is `max_file_size * max_files`. 
With `file_padding=5` the max number of files is 10,000. 

A DB created with `file_padding=5` and `max_file_size=16` is capable to store up to 160 GB, or 167,772,160,000 items. 

At its maximum capacity will allocate 10,000 files.

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

Donate :heart: **Bitcoin** to: 3FVGopUDc6tyAP6t4P8f3GkYTJ5JD5tPwV or [paypal](https://paypal.me/gdax)