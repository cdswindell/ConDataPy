from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Tuple


class Derivable(ABC):
    __slots__: Tuple[()] = ()

    @abstractmethod
    def clear_derivation(self) -> None:
        pass


class DerivableThreadPool(ABC):
    pass


class DerivableThreadPoolConfig(ABC):
    pass
