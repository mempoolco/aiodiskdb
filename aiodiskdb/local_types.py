import asyncio
from dataclasses import dataclass

import typing
from enum import Enum


@dataclass
class ItemLocation:
    index: int
    position: int
    size: int


class LockType(Enum):
    READ = 0
    WRITE = 1


@dataclass
class Buffer:
    index: int
    data: bytes
    size: int
    items: int
    file_size: int


@dataclass
class TempBufferData:
    buffer: typing.Optional[Buffer]
    idx: typing.Dict


@dataclass
class EventsHandlers:
    """
    Callback signature, first argument is always the execution timestamp (time.time()).

    async def callback(fired_at: int, *callback_data):
        pass
    """
    on_start: typing.Optional[callable] = None
    on_stop: typing.Optional[callable] = None
    on_failure: typing.Optional[callable] = None
    on_index_drop: typing.Optional[callable] = None
    on_write: typing.Optional[callable] = None

    def __setattr__(self, key, value):
        if value and not asyncio.iscoroutinefunction(value):
            raise TypeError(f'{key} must be a coroutine')
        self.__dict__[key] = value


class TransactionStatus(Enum):
    INITIALIZED = 1
    ONGOING = 2
    DONE = 3
