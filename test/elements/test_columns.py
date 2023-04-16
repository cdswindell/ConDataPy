from __future__ import annotations

from numbers import Number

import pytest

from cdspy.examples import NumericRangeRequired, NumericRange
from cdspy.templates import ConstraintViolationError
from ..test_base import TestBase

from cdspy.elements import Table
from cdspy.elements import Column


# noinspection PyMethodMayBeStatic,PyTypeChecker
class TestColumns(TestBase):
    def test_column_properties(self) -> None:
        c = Column(None)  # type: ignore[arg-type]
        assert c.is_initializing
        assert not c.is_initialized

        for p in c.properties:
            print(f"{p} -> {c.get_property(p)}")
            assert c.has_property(p) or c.get_property(p) is None
