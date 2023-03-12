from __future__ import annotations

from threading import Lock
from typing import Optional


class AtomicInteger:
    def __init__(self, value: int = 0) -> None:
        self._value = int(value)
        self._lock = Lock()

    def inc(self, d: Optional[int] = 1) -> int:
        d = d if d is not None else 1
        with self._lock:
            retval = self._value
            self._value += int(d)
            return retval

    def dec(self, d: Optional[int] = 1) -> int:
        d = d if d is not None else 1
        with self._lock:
            retval = self._value
            self.inc(-d)
            return retval

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    # setter is not used in cdspy, but is included for completeness here
    # @value.setter
    # def value(self, v):
    #     with self._lock:
    #         self._value = int(v)
    #         return self._value
