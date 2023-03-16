from __future__ import annotations

from collections.abc import Collection, MutableSequence
from typing import Generic, Optional, TypeVar

_T = TypeVar("_t")


class ArrayList(MutableSequence, Generic[_T]):
    __slots__ = ["_list", "_len"]

    def __init__(self, initial_capacity: Optional[int] = 0) -> None:
        super().__init__()
        self._len = 0
        if initial_capacity:
            self._list = [None] * initial_capacity
        else:
            self._list = None

    def __len__(self) -> int:
        return self._len

    def capacity(self) -> int:
        if self._list:
            return len(self._list)
        else:
            return 0