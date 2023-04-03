from __future__ import annotations

import sys

from collections.abc import Collection, MutableSequence
from threading import RLock
from typing import Any, cast, Final, Generic, List, Optional, TypeVar, overload, Iterable, Iterator

_DEFAULT_CAPACITY_INCREMENT: Final = 16
_T = TypeVar("_T")


class KeyCompare(Generic[_T]):
    def __init__(self, source: _T | None) -> None:
        super().__init__()
        if source is None:
            n: Any = sys.maxsize
            s = ""
        else:
            s = str(source).lower() if source else ""
            n: Any = source if isinstance(source, int) else source if isinstance(source, float) else sys.maxsize - 1
        self._key = (n, s)

    @property
    def key(self) -> Any:
        return self._key


class ArrayList(MutableSequence, Generic[_T]):
    __slots__ = ["_capacity_incr", "_list", "_len", "_iter_idx", "_lock"]

    def __init__(
        self,
        source: Optional[Collection[_T]] = None,
        initial_capacity: int = 0,
        capacity_increment: int = _DEFAULT_CAPACITY_INCREMENT,
    ) -> None:
        super().__init__()
        if initial_capacity is not None and (not isinstance(initial_capacity, int) or int(initial_capacity) < 0):
            raise ValueError("initial_capacity must be >= 0")
        if capacity_increment is not None and (not isinstance(capacity_increment, int) or int(capacity_increment) < 0):
            raise ValueError("capacity_increment must be >= 0")

        initial_capacity = initial_capacity if initial_capacity is not None else 0
        self._capacity_incr = capacity_increment if capacity_increment is not None else _DEFAULT_CAPACITY_INCREMENT
        self._len = 0
        self._iter_idx = 0
        self._list: List[_T] = []
        if source:
            if isinstance(source, Collection):
                self._list.extend(source)
                self._len = len(self._list)
            else:
                raise NotImplementedError(f"Can not create ArrayList from '{type(source).__name__}'")
        elif isinstance(initial_capacity, int):
            self._list.extend(cast(List[_T], [None] * initial_capacity))
        self._lock = RLock()

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
                return self._list.__getitem__(self._get_effective_index(index))
        elif isinstance(index, slice):
            return self._list[0 : self._len].__getitem__(index)
        else:
            raise TypeError(f"ArrayList indices must be integers or slices, not {type(index).__name__}")

    @overload
    def __setitem__(self, index: int, value: _T) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[_T]) -> None:
        ...

    def __setitem__(self, index: int | slice, value: _T | Iterable[_T]) -> None:
        if isinstance(index, int):
            self.ensure_capacity(abs(index) + 1)
            if index >= 0:
                self._list.__setitem__(index, cast(_T, value))
                if index + 1 > self._len:
                    self._len = index + 1
            else:
                if self._len <= 0 or abs(index) > self._len:
                    raise IndexError("ArrayList assignment index out of range")
                self._list.__setitem__(self._get_effective_index(index), cast(_T, value))
                if abs(index) > self._len:
                    self._len = abs(index)
        elif isinstance(index, slice):
            capacity = self.capacity
            new_list = list(self)
            new_list[index] = cast(Iterable[_T], value)
            self._len = len(new_list)
            self._list = new_list
            self.ensure_capacity(capacity)
        else:
            raise TypeError(f"ArrayList indices must be integers or slices, not {type(index).__name__}")

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
                self._list.__delitem__(self._get_effective_index(index))
                self._len -= 1
        elif isinstance(index, slice):
            if self._list:
                new_list = list(self)
                new_list.__delitem__(index)
                self._len = len(new_list)
                self._list = new_list
        else:
            raise TypeError(f"ArrayList indices must be integers or slices, not {type(index).__name__}")
        self.ensure_capacity(capacity)

    def __eq__(self, o: object) -> bool:
        if o is None or not isinstance(o, ArrayList):
            return False
        return self._list[0 : self._len] == cast(ArrayList[_T], o)._list[0 : len(o)]

    def __ne__(self, o: object) -> bool:
        return not self.__eq__(o)

    def __add__(self, other: _T | Iterable[_T]) -> ArrayList[_T]:
        if isinstance(other, ArrayList):
            capacity_incr = max(self.capacity_increment, other.capacity_increment)
            return ArrayList[_T](
                self._list[0 : self._len] + other._list[0 : len(other)], capacity_increment=capacity_incr
            )
        elif isinstance(other, Collection):
            return ArrayList[_T](self._list[0 : self._len] + list(other), capacity_increment=self.capacity_increment)
        else:
            nl = list(self)
            nl.append(other)
            return ArrayList[_T](nl, capacity_increment=self.capacity_increment)

    def __iadd__(self, other: _T | Iterable[_T]) -> ArrayList[_T]:
        if isinstance(other, ArrayList):
            self.extend(other[:])
        elif isinstance(other, Collection):
            self.extend(other)
        else:
            self.append(cast(_T, other))
        return self

    def __mul__(self, n: int) -> ArrayList[_T]:
        return ArrayList[_T](self._list[0 : self._len] * int(n), capacity_increment=self.capacity_increment)

    __rmul__ = __mul__

    def __imul__(self, n: int) -> ArrayList[_T]:
        self._list = self._list[0 : self._len] * int(n)
        self._len = self._len * n if n >= 0 else 0
        return self

    def __copy__(self) -> ArrayList[_T]:
        return ArrayList[_T](self, self.capacity, self.capacity_increment)

    def _get_effective_index(self, index: int, max_value: Optional[int] = None) -> int:
        if not isinstance(index, int):
            raise TypeError(f"'{type(index).__name__} object cannot be interpreted as an integer")
        index = index if index >= 0 else index - self.capacity + self._len
        if max_value is not None and int(max_value) >= 0:
            if index > max_value:
                index = max_value
        return index

    @property
    def lock(self) -> RLock:
        return self._lock

    def append(self, value: _T) -> None:
        self.ensure_capacity(self._len + 1)
        self._list.__setitem__(self._len, value)
        self._len += 1

    def extend(self, values: Iterable[_T]) -> None:
        for v in values:
            self.append(v)

    def index(self, value: _T, start: int = 0, stop: Optional[int] = None, /) -> int:
        try:
            return self._list.index(value, self._get_effective_index(start), stop if stop else self._len)
        except ValueError:
            raise ValueError(f"{value} is not in the ArrayList")

    def insert(self, idx: int, value: _T) -> None:
        if self._len:
            index = self._get_effective_index(idx, self._len)
        else:
            index = 0
        self._list.insert(index, value)
        self._len += 1
        self.trim_to_capacity()

    def clear(self) -> None:
        self._list.clear()
        self._len = 0
        self._iter_idx = 0
        if self.capacity_increment:
            self.ensure_capacity()

    def pop(self, index: int = -1) -> _T:
        if not isinstance(index, int):
            raise TypeError(f"'{type(index).__name__} object cannot be interpreted as an integer")
        if not self._list:
            raise IndexError("pop from empty ArrayList")
        if index >= self._len or index < -self._len:
            raise IndexError("pop index out of range")
        index = self._get_effective_index(index) if index is not None else self._len - 1
        # we now know index is good; pop the backing list
        capacity = self.capacity - 1
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

    def count(self, value: Any) -> int:
        return self._list.count(value)

    def copy(self) -> ArrayList[_T]:
        return ArrayList[_T](self)

    def sort(self, /, *args: Any, **kwargs: Any) -> None:
        if self._list:
            self._list[0 : self._len] = sorted(self._list[0 : self._len], *args, **kwargs)

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

    def trim(self) -> None:
        if self._len < self.capacity:
            del self._list[self._len :]

    def trim_to_capacity(self) -> None:
        self.trim()
        self.ensure_capacity()
