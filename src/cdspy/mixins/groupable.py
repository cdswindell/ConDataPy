from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from cdspy.elements import Group


class Groupable(ABC):
    __slots__: Tuple[()] = ()

    @abstractmethod
    def _add_to_group(self, g: Group) -> None:
        pass

    @abstractmethod
    def _remove_from_group(self, g: Group) -> None:
        pass
