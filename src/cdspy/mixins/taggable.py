from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Tuple, Collection


class Taggable(ABC):
    __slots__: Tuple[()] = ()

    @property
    @abstractmethod
    def tags(self) -> Collection[str]:
        ...

    @tags.setter
    @abstractmethod
    def tags(self, *tags: str) -> None:
        ...
