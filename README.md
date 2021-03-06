![aiodiskdb logo](./docs/logo128.png "aiodiskdb")
### Minimal, embeddable on-disk DB, tailored for asyncio.

---
[![Coverage Status](https://coveralls.io/repos/github/mempoolco/aiodiskdb/badge.svg?branch=main)](https://coveralls.io/github/mempoolco/aiodiskdb?branch=main)
[![PyPI version](https://badge.fury.io/py/aiodiskdb.svg)](https://badge.fury.io/py/aiodiskdb)
[![PyPI license](https://img.shields.io/pypi/l/aiodiskdb.svg)](https://pypi.python.org/pypi/aiodiskdb/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/aiodiskdb.svg)](https://pypi.python.org/pypi/aiodiskdb/)
[![Build Status](https://travis-ci.com/mempoolco/aiodiskdb.svg?branch=main)](https://travis-ci.com/mempoolco/aiodiskdb)
[![Chat on Telegram](https://img.shields.io/badge/Chat%20on-Telegram-brightgreen.svg)](https://t.me/mempoolco)
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
async def callback(timestamp: int, event: WriteEvent):
    human_time = datetime.fromtimestamp(timestamp).isoformat()
    log(f'{human_time} - {event} persisted to disk.')
    await do_something(location)
    
db.events.on_write = callback
```

Or hook to other events:
```python
db.events.on_start = ...
db.events.on_stop = ...
db.events.on_failure = ...
db.events.on_index_drop = ...
```

### Asynchronous non-blocking

Handle file writes with no locks. 
Data is appended in RAM and persisted asynchronously, according to customizable settings. 

### Transactional

"All or nothing" commit. 
Lock all the DB write operations during commits, still allowing the reads.
Ensure an arbitrary sequence of data is persisted to disk.

Transaction is scoped. Data added into a transaction is not available outside until committed.
```python
transaction = await db.transaction()

transaction.add(b'cafe')
transaction.add(b'babe')
transaction.add(b'deadbeef')

locations: typing.Sequence[ItemLocation] = await transaction.commit()
```


### Not-so-append-only

**Aiodiskdb** is an append-only database. It means you'll never see methods to *delete* or *remove* single entries.

However, data pruning is supported, with the following methods:

```python
db.enable_overwrite()
db.rtrim(0, 400)
db.ltrim(8, 900)
db.drop_index(3)
db.disable_overwrite()
```

These three methods respectively:
- prune data from the right, at index `0`, starting from the location `400` to the index end (`rtrim`)
- prune data from the left, at index `8`, starting from the beginning to the location `900` (`ltrim`)
- drop the whole index `3`, resulting in a file deletion: `drop_index`

All the items locations not involved into a TRIM operation remains unmodified, even after an `ltrim`.

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

A DB created with `file_padding=5` and `max_file_size=16` is capable to store up to 160 GB, or 167,772,160,000 items, 
at its maximum capacity will allocate 10,000 files.

### Try to do its best

Hook the blocking `on_stop_signal` method to avoid data losses on exit.
```python
import signal
from aiodiskdb import AioDiskDB

db = AioDiskDB(...)

signal.signal(signal.SIGINT, db.on_stop_signal)
signal.signal(signal.SIGTERM, db.on_stop_signal)
signal.signal(signal.SIGKILL, db.on_stop_signal)
```

### Quite enough fast for some use cases

![aiodiskdb files](./docs/aiodiskdb.gif)

Concurrency tests, part of the unit tests, can be replicated as system benchmark.
The followings are performed on a common consumer SSD:
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

### Limitations

```python
assert len(data) <= max_buffer_size
assert max_transaction_size < RAM
assert max_file_size < 4096
```

If `rtrim` is applied on the **current** index, the space is reused, otherwise no. 
With `ltrim`, once the space is freed, it is not allocated again. 
With `drop_index` the discarded index is not reused.

With a lot of data turn-over (pruning by trimming), it may be necessary to set an unusual high `file_padding`, and
increase the database potential size. 

---

### Credits

Inspired by the raw block data storage of the [bitcoincore blocks database](https://en.bitcoin.it/wiki/Bitcoin_Core_0.11_(ch_2):_Data_Storage). 

Logo by mepheesto.

### Notes

**Alpha stage. Still under development, use with care and expect data losses.**

Donate :heart: **Bitcoin** to: 3FVGopUDc6tyAP6t4P8f3GkYTJ5JD5tPwV or [paypal](https://paypal.me/gdax)
