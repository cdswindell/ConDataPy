from __future__ import annotations

import pytest

from cdspy.examples import NumericRangeRequired, NumericRange
from cdspy.templates import ConstraintViolationError
from ..test_base import TestBase

from cdspy.elements import Table


# noinspection PyMethodMayBeStatic
class TestCellValidation(TestBase):
    def test_cell_validation(self) -> None:
        t = Table()
        r1 = t.add_row()
        r2 = t.add_row()
        r3 = t.add_row()
        c1 = t.add_column()
        c2 = t.add_column()

        r1.cell_validator = NumericRange(30.0, 40.0)
        c1.cell_validator = NumericRange(1, 10)

        c_r1_c1 = t.get_cell(r1, c1)
        c_r2_c1 = t.get_cell(r2, c1)
        c_r3_c1 = t.get_cell(r3, c1)
        c_r3_c1.validator = NumericRangeRequired(-100, 20)

        # now set values
        c_r1_c1.value = 2.0
        assert c_r1_c1.value == 2.0

        c_r2_c1.value = None
        assert c_r2_c1.value is None

        c_r3_c1.value = 19
        assert c_r3_c1.value == 19

        # these tests should fail
        with pytest.raises(ConstraintViolationError, match="Too Small"):
            c_r2_c1.value = -5

        with pytest.raises(ConstraintViolationError, match="Required"):
            c_r3_c1.value = None

        c_r1_c2 = t.get_cell(r1, c2)
        c_r2_c2 = t.get_cell(r2, c2)
        c_r3_c2 = t.get_cell(r3, c2)

        c_r1_c2.value = 35
        assert c_r1_c2.value == 35

        c_r2_c2.value = 200
        assert c_r2_c2.value == 200

        c_r3_c2.value = None
        assert c_r3_c2.is_null
        assert c_r3_c2.value is None

        # these tests should fail
        with pytest.raises(ConstraintViolationError, match="Too Large"):
            c_r1_c2.value = 50

        with pytest.raises(ConstraintViolationError, match="Numeric Value Required"):
            c_r1_c2.value = "abc"

        # clear validator, should now be no exceptions
        r1.cell_validator = None
        c_r1_c2.value = 50
        assert c_r1_c2.value == 50

        c_r1_c2.value = 0
        assert c_r1_c2.value == 0

        c_r1_c2.value = "abc"
        assert c_r1_c2.value == "abc"

    def test_lamda_transformer(self) -> None:
        t = Table()
        r1 = t.add_row()
        r2 = t.add_row()
        r3 = t.add_row()

        c1 = t.add_column()
        c1.cell_transformer = lambda x: x.upper()
        assert c1.cell_transformer is not None

        c1.fill("abc")
        for r in [r1, r2, r3]:
            assert t.get_cell_value(r, c1) == "ABC"

        # clear validator, no transform should occur
        c1.cell_transformer = None
        c1.fill("abc")
        for r in [r1, r2, r3]:
            assert t.get_cell_value(r, c1) == "abc"
