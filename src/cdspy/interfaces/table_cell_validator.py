from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class TableCellValidator(ABC):
    @abstractmethod
    def validate(self, new_value: Any) -> None:
        ...

    def transform(self, new_value: Any) -> Any:
        self.validate(new_value)
        return new_value


class ConstraintViolationError(ValueError):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args)
