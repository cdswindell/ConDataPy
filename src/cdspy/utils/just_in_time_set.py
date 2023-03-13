from __future__ import annotations

from collections.abc import Collection, MutableSet
from threading import RLock
from typing import Iterator, Optional, TypeVar
from weakref import WeakSet

V = TypeVar('V', bound=object)


class JustInTimeSet(MutableSet):
    __slots__ = ('_backing_set', '_lock')

    def __init__(self, elems: Optional[Collection[V]] = None) -> None:
        if elems:
            self._backing_set: WeakSet[V] | None = WeakSet(elems)
        else:
            self._backing_set: WeakSet[V] | None = None
        self._lock = RLock()

    def __contains__(self, x: V) -> bool:
        with self._lock:
            return x in self._backing_set if self._backing_set else False

    def __len__(self) -> int:
        with self._lock:
            return len(self._backing_set) if self._backing_set else 0

    def __iter__(self) -> Iterator[V]:
        with self._lock:
            if self._backing_set:
                for elem in [x for x in self._backing_set]:
                    if elem and elem.is_valid:
                        yield elem
            else:
                yield from ()

    def __create_backing_set(self) -> None:
        with self._lock:
            if self._backing_set is None:
                self._backing_set: WeakSet[V] | None = WeakSet()

    def add(self, value: V) -> None:
        with self._lock:
            self.__create_backing_set()
            self._backing_set.add(value)

    def discard(self, value: V) -> None:
        with self._lock:
            if self._backing_set:
                self._backing_set.discard(value)

    def remove(self, value: V) -> None:
        with self._lock:
            if self._backing_set:
                self._backing_set.remove(value)
            else:
                raise KeyError(value)

    def clear(self) -> None:
        with self._lock:
            if self._backing_set:
                self._backing_set.clear()

    def pop(self) -> V:
        with self._lock:
            if self._backing_set:
                return self._backing_set.clear.pop
        raise KeyError("pop from empty set")
