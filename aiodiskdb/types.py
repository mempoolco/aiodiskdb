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
