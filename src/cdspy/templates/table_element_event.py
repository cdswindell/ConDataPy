from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ..elements import EventType


class TableElementEvent(ABC):
    __slots__: Tuple[()] = ()

    @property
    @abstractmethod
    def event_type(self) -> EventType:
        ...
