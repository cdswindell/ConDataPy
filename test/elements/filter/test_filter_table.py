from __future__ import annotations

import pytest

from ...test_base import TestBase

from cdspy.elements import Table, Group
from cdspy.elements.filters import FilteredTable


class TestFilterTable(TestBase):
    def test_filter_table(self) -> None:
        t = Table()
        c1 = t.add_column()
        c2 = t.add_column()
        c3 = t.add_column()
        r50 = t.add_row(50)
        t.fill(34)

        g = Group(t, None, c1, c3)
        assert g
        assert g.num_columns == 2
        assert g.num_rows == 0
        assert g._num_effective_rows == 50

        ft = FilteredTable.create_table(t, g)
        assert ft
        assert ft.num_columns == 2
        assert ft.num_rows == 50

        assert ft.parent == t
        cell_cnt = 0
        for cell in ft.cells:
            assert cell
            assert cell.value == 34
            cell_cnt += 1
        assert cell_cnt == ft.num_rows * ft.num_columns
