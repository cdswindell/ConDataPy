from __future__ import annotations

from collections.abc import MutableSequence
from typing import Any, cast, Generic, Optional, TypeVar, overload, Iterable, Iterator

_T = TypeVar("_T")


class ArrayList(MutableSequence, Generic[_T]):
    __slots__ = ["_capacity_incr", "_list", "_len", "_iter_idx"]

    def __init__(
        self,
        source: Optional[MutableSequence[_T]] = None,
        initial_size: int = 0,
        capacity_incr: int = 256,
    ) -> None:
        super().__init__()
        self._capacity_incr = capacity_incr if capacity_incr else 256
        self._len = 0
        self._iter_idx = 0
        if source:
            if isinstance(source, Iterable):
                self._list: MutableSequence[_T] = cast(MutableSequence[_T], list(source))
                self._len = len(self._list)
            else:
                raise NotImplementedError(f"Can not create ArrayList from {type(source)}")
        elif isinstance(initial_size, int) and initial_size >= 0:
            self._list: MutableSequence[_T] = cast(MutableSequence[_T], [None] * initial_size)
        else:
            self._list = cast(MutableSequence[_T], [None] * self.capacity_increment)

    def __repr__(self) -> str:
        return f"[{', '.join([str(x) for x in self._list[0:self._len]])}]"

    def __len__(self) -> int:
        return self._len

    def __iter__(self) -> Iterator[_T]:
        self._iter_idx = 0
        return self

    def __next__(self) -> _T:
        if self._iter_idx >= self._len:
            raise StopIteration
        else:
            self._iter_idx += 1
            return self._list[self._iter_idx - 1]

    @overload
    def __getitem__(self, index: int) -> _T:
        ...

    @overload
    def __getitem__(self, index: slice) -> MutableSequence[_T]:
        ...

    def __getitem__(self, index: int | slice) -> _T | MutableSequence[_T]:
        if isinstance(index, int):
            if self._len == 0 or index > self._len or index < -self._len:
                raise IndexError("ArrayList index out of range")
            else:
                return self._list.__getitem__(index)
        elif isinstance(index, slice):
            return list(self).__getitem__(index)
        else:
            raise TypeError(f"ArrayList indices must be integers or slices, not {type(index)}")

    @overload
    def __setitem__(self, index: int, value: _T) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[_T]) -> None:
        ...

    def __setitem__(self, index: int | slice, value: _T | Iterable[_T]) -> None:
        if isinstance(index, int):
            if self._len == 0 or index > self._len or index < -self._len:
                raise IndexError("ArrayList assignment index out of range")
            else:
                self._list[index] = cast(_T, value)
        elif isinstance(index, slice):
            capacity = self.capacity
            new_list = list(self)
            new_list[index] = cast(Iterable[_T], value)
            self._len = len(new_list)
            self._list = new_list
            self.ensure_capacity(capacity)
        else:
            raise TypeError(f"ArrayList indices must be integers or slices, not {type(index)}")

    @overload
    def __delitem__(self, index: int) -> None:
        ...

    @overload
    def __delitem__(self, index: slice) -> None:
        ...

    def __delitem__(self, index: int | slice) -> None:
        capacity = self.capacity
        if isinstance(index, int):
            if self._len == 0 or index > self._len or index < -self._len:
                raise IndexError("ArrayList assignment index out of range")
            else:
                self._list.__delitem__(index)
                self._len -= 1
        elif isinstance(index, slice):
            if self._list:
                new_list = list(self)
                new_list.__delitem__(index)
                self._len = len(new_list)
                self._list = new_list
        else:
            raise TypeError(f"ArrayList indices must be integers or slices, not {type(index)}")
        self.ensure_capacity(capacity)

    @property
    def capacity(self) -> int:
        return len(self._list) if self._list else 0

    @property
    def capacity_increment(self) -> int:
        return self._capacity_incr

    @capacity_increment.setter
    def capacity_increment(self, increment: int) -> None:
        if int(increment) < 0:
            raise ValueError("Increment must be >= 0")
        self._capacity_incr = increment

    def index(self, value: _T, start: int = 0, stop: Optional[int] = None, /) -> int:
        try:
            return self._list.index(value, start, stop if stop else self._len)
        except ValueError:
            raise ValueError(f"{value} is not in the ArrayList")

    def insert(self, idx: int, value: _T) -> None:
        if idx < self._len:
            self._list.insert(idx, value)
            self._len += 1

    def extend(self, values: Iterable[_T]) -> None:
        for v in values:
            self.append(v)

    def clear(self) -> None:
        self._list.clear()
        self._len = 0
        self._iter_idx = 0
        if self.capacity_increment:
            self.ensure_capacity()

    def pop(self, index: Optional[int] = None) -> _T:
        if not self._list:
            raise IndexError("pop from empty ArrayList")
        index = index if index is not None else self._len - 1
        if index >= self._len or index < -self._len:
            raise IndexError("pop index out of range")
        # we now know index is good; pop the backing list
        capacity = self.capacity
        val = self._list.pop(index)
        # fix up length and capacity
        self._len -= 1
        self.ensure_capacity(capacity)
        # return popped value
        return val

    def remove(self, item: _T) -> None:
        try:
            capacity = self.capacity
            self._list.remove(item)
            self._len -= 1
            self.ensure_capacity(capacity)
        except ValueError:
            raise ValueError("ArrayList.remove(x) x not in ArrayList")

    def reverse(self) -> None:
        if self._list:
            self._list[0 : self._len] = self._list[0 : self._len][::-1]

    def append(self, value: _T) -> None:
        self.ensure_capacity(self._len + 1)
        self._list.__setitem__(self._len, value)
        self._len += 1

    def count(self, value: Any) -> int:
        return self._list.count(value)

    def ensure_capacity(self, capacity: Optional[int] = None) -> None:
        capacity = capacity if capacity else self.capacity_increment
        # the new capacity must be at least the same as the old, and,
        # if _capacity_incr is defined, a multiple of it
        if self.capacity < capacity or (self._capacity_incr > 0 and self.capacity % self._capacity_incr != 0):
            # how many elements do we need to get back to where we were?
            needed_elements = max(0, capacity - self.capacity)
            if self._capacity_incr > 1:
                # if capacity increment is specified, round
                # needed elements to keep it a multiple of capacity_incr
                rounding = max(capacity, self.capacity) % self._capacity_incr
                needed_elements += self._capacity_incr - rounding if rounding else 0
            self._list.extend(cast(MutableSequence[_T], [None] * needed_elements))
