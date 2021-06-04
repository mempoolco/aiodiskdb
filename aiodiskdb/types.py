from dataclasses import dataclass

import typing


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
class CurrentLocation:
    index: int
    position: int
    size_bytes: int
