from __future__ import annotations

import gc
import pytest

from ..test_base import TestBase

from cdspy.elements import TableContext, Access


# noinspection PyMethodMayBeStatic
class TestWeakReferences(TestBase):
    def test_nonpersistent_tables(self) -> None:
        tc = TableContext()
        num_tables = 16
        num_rows = 256  # 1024
        num_cols = 3  # 256

        tc.row_capacity_incr_default = num_rows
        tc.column_capacity_incr_default = num_cols

        # create a number of large , non-persistent tables
        for _ in range(0, num_tables):
            t = self.create_large_table(tc, num_rows, num_cols, "really big table!!")
            assert t.num_rows == num_rows
            assert t.num_columns == num_cols
            t = None  # type: ignore[assignment]

        # they should all be gone on a gc
        gc.collect()
        assert len(tc) == 0

    @pytest.mark.skip(reason="no way of currently testing this")
    def test_persistent_tables(self) -> None:
        tc = TableContext()
        num_tables = 8
        num_rows = 1024
        num_cols = 2562

        tc.row_capacity_incr_default = num_rows
        tc.column_capacity_incr_default = num_cols
        tc.is_tables_persistent_default = True

        # create a number of large , non-persistent tables
        for _ in range(0, num_tables):
            t = self.create_large_table(tc, num_rows, num_cols, "really big table!!")
            assert t.num_rows == num_rows
            assert t.num_columns == num_cols
            assert t.is_persistent
            t = None  # type: ignore[assignment]

        assert len(tc) == num_tables

        # they should all be here on a gc
        gc.collect()
        assert len(tc) == num_tables

        # clear persistent flag; they should go away
        t = tc.get_table(Access.ByLabel, "large table")  # type: ignore[assignment]
        while t:
            t.is_persistent = False
            t = None  # type: ignore[assignment]
            gc.collect()
            t = tc.get_table(Access.ByLabel, "large table")  # type: ignore[assignment]

        gc.collect()
        assert len(tc) == 0
