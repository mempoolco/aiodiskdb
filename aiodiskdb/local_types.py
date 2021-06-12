import asyncio
from dataclasses import dataclass

import typing
from enum import Enum


@dataclass
class ItemLocation:
    index: int
    position: int
    size: int

    def serialize(self):
        return self.index.to_bytes(4, 'little') + \
            self.position.to_bytes(4, 'little') + \
            self.size.to_bytes(4, 'little')

    @classmethod
    def deserialize(cls, location: bytes):
        return cls(
            index=int.from_bytes(location[:4], 'little'),
            position=int.from_bytes(location[4:8], 'little'),
            size=int.from_bytes(location[8:12], 'little'),
        )


class LockType(Enum):
    READ = 0
    WRITE = 1
    TRANSACTION = 2


@dataclass
class Buffer:
    index: int
    data: bytes
    size: int
    items: int
    file_size: int
    offset: int
    head: bool


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
        if value is not None and not asyncio.iscoroutinefunction(value):
            raise TypeError(f'{key} must be a coroutine')
        self.__dict__[key] = value


class TransactionStatus(Enum):
    INITIALIZED = 1
    ONGOING = 2
    DONE = 3


@dataclass
class WriteEvent:
    index: int
    position: int
    size: int


@dataclass
class FileHeader:
    genesis_bytes: bytes
    trim_offset: int

    def serialize(self) -> bytes:
        return self.genesis_bytes + \
               int(self.trim_offset).to_bytes(4, 'little') + \
               int(0).to_bytes(16, 'little')  # reserved
