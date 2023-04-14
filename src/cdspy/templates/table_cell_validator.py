from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable


class TableCellValidator(ABC):
    @abstractmethod
    def validate(self, value: Any) -> None:
        ...

    def transform(self, value: Any) -> Any:
        self.validate(value)
        return value


class LambdaValidator(TableCellValidator):
    @classmethod
    def build(cls, lfn: Callable) -> TableCellValidator:
        return LambdaValidator(lfn)

    def __init__(self, lfn: Callable):
        self._function = lfn

    def validate(self, value: Any) -> None:
        self._function(value)


class ConstraintViolationError(ValueError):
    @classmethod
    def raise_(cls, *args: Any) -> None:
        raise cls(*args)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args)
