from __future__ import annotations

from numbers import Number
from typing import Any

from ..templates.table_cell_validator import TableCellValidator
from ..templates.table_cell_validator import ConstraintViolationError


# noinspection PyTypeChecker
class NumericRange(TableCellValidator):
    def __init__(self, min_value: float, max_value: float) -> None:
        if max_value < min_value:
            raise ValueError("Minimum value must be less than or equal to maximum value.")
        self._min_value = min_value
        self._max_value = max_value

    def validate(self, value: Any) -> None:
        if value is None:
            return
        if not isinstance(value, Number):
            raise ConstraintViolationError("Numeric Value Required")
        if value < self._min_value:  # type: ignore[operator]
            raise ConstraintViolationError("Too Small")
        if value > self._max_value:  # type: ignore[operator]
            raise ConstraintViolationError("Too Large")


class NumericRangeRequired(NumericRange):
    def __init__(self, min_value: float, max_value: float) -> None:
        super().__init__(min_value, max_value)

    def validate(self, value: Any) -> None:
        if value is None:
            raise ConstraintViolationError("Required")
        super().validate(value)
