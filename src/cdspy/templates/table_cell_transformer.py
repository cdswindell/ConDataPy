from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable

from . import TableCellValidator


class TableCellTransformer(TableCellValidator, ABC):
    @abstractmethod
    def transform(self, value: Any) -> Any:
        ...

    def validate(self, value: Any) -> None:
        pass  # intentionally a NOOP


class LambdaTransformer(TableCellTransformer):
    @classmethod
    def build(cls, lfn: Callable) -> TableCellTransformer:
        return LambdaTransformer(lfn)

    def __init__(self, lfn: Callable):
        self._function = lfn

    def transform(self, value: Any) -> Any:
        return self._function(value)
