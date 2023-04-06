from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from . import TableCellValidator


class TableCellTransformer(TableCellValidator, ABC):
    @abstractmethod
    def transform(self, new_value: Any) -> Any:
        ...

    def validate(self, new_value: Any) -> None:
        pass  # intentionally a NOOP
