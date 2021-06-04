from dataclasses import dataclass

import typing
from enum import Enum


@dataclass
class ItemLocation:
    index: int
    position: int
    length: typing.Union[None, int]

    @property
    def serialized(self):
        if not self.length:
            raise ValueError('Cannot serialize item with no length')
        return int.to_bytes(self. index, 4, 'little') + \
            int.to_bytes(self.position, 4, 'little') + \
            int.to_bytes(self.length, 4, 'little')


@dataclass
class Location:
    index: int
    position: int
    size_bytes: int


class LockType(Enum):
    READ = 0
    WRITE = 1
