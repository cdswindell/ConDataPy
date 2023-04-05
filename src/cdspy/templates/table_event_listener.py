from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from . import TableElementEvent


class TableEventListener(ABC):
    __slots__: Tuple[()] = ()

    @abstractmethod
    def event_occurred(self, e: TableElementEvent) -> None:
        ...
