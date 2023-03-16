from __future__ import annotations

from abc import ABC, abstractmethod


class Derivable(ABC):
    @abstractmethod
    def clear_derivation(self) -> None:
        pass


class DerivableThreadPool(ABC):
    pass


class DerivableThreadPoolConfig(ABC):
    pass
