from __future__ import annotations

import gc

import pytest

from ..test_base import TestBase

from cdspy.elements import Table, Group
from cdspy.exceptions import InvalidParentException


# noinspection PyUnusedLocal,PyUnresolvedReferences
class TestGroups(TestBase):
    def test_group_properties(self) -> None:
        t = Table()
        g = Group(t)  # type: ignore[arg-type]
        assert not g.is_initializing
        assert g.is_initialized

        for p in g.properties:
            print(f"{p} -> {g.get_property(p)}")
            assert g.has_property(p) or g.get_property(p) is None

    def test_create_group(self) -> None:
        t = Table(10, 10)
        assert t
        assert t.num_groups == 0

        g = Group(t)
        assert g
        assert g.num_rows == 0
        assert g.num_columns == 0
        assert g.num_cells == 0
        assert g.num_groups == 0

        assert g.table == t
        assert g.table_context == t.table_context
        assert t.num_groups == 1

        t._deregister_group(g)
        assert t.num_groups == 0

        # test weak reference behavior
        t._register_group(g)
        assert t.num_groups == 1

        g = None  # type: ignore[assignment]
        gc.collect()
        assert t.num_groups == 0

    def test_grouped_rows(self) -> None:
        t = Table(100, 100)
        assert t
        assert t.num_groups == 0

        g = Group(t)

        # make sure we cant add g to itself
        with pytest.raises(RecursionError):
            g.add(g)

        # create some rows and add them to group
        r1 = t.add_row()
        r2 = t.add_row()
        r3 = t.add_row()

        rs = {r1, r2, r3}
        g.update(rs)
        assert g.num_rows == 3
        assert r1 in g
        assert r2 in g
        assert r3 in g

        r1g = r1.groups
        assert r1g
        assert len(r1g) == 1
        assert g in r1g

        # assert list of groups is immutable
        with pytest.raises(AttributeError):
            r1g.clear()  # type: ignore[attr-defined]

        with pytest.raises(TypeError):
            r1g[0] = None  # type: ignore[index]

        # test that elements from another table cannot be added
        t2 = Table(100, 100)
        rt2 = t2.add_row()
        with pytest.raises(InvalidParentException):
            g.add(rt2)

        # remove rows individually
        g.remove(r1)
        assert g.num_rows == 2

        g.remove(r2, r3)
        assert g.num_rows == 0

    def test_effective_elements(self) -> None:
        t = Table()
        r1 = t.add_row()
        r2 = t.add_row()
        r3 = t.add_row()
        r4 = t.add_row()
        r5 = t.add_row()

        c1 = t.add_column()
        c2 = t.add_column()
        c3 = t.add_column()

        g = Group(t)
        assert g

        # add c1 and c2 to group; as there are 5 rows in the parent table,
        # g.num_rows should be 5
        g.add(c1, c2)
        assert g._num_effective_columns == 2
        assert g._num_effective_rows == 5
        assert g.num_cells == 5 * 2

        # add one row to group; effective rows should now be 1
        g.add(r2)
        assert g._num_effective_columns == 2
        assert g._num_effective_rows == 1
        assert g.num_cells == 1 * 2

        # add two more rows to group
        g.add(r1, r4)
        assert g._num_effective_columns == 2
        assert g._num_effective_rows == 3
        assert g.num_cells == 3 * 2

        # remove the columns from the group; effective columns should now be 3
        g.remove(c1, c2)
        assert g._num_effective_columns == 3
        assert g._num_effective_rows == 3
        assert g.num_cells == 3 * 3

    def test_grouped_elements(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)

        c5 = t.add_column(5)
        c10 = t.add_column(10)
        c15 = t.add_column(15)

        # create the main test group; until explicit rows are added to it,
        # it defines a 2 column by 200 row slice of cells
        g = Group(t)
        g.add(c5, c10)
        assert g.num_cells == t.num_rows * 2

        # add rows 1 and 2 and c15 to group; should reduce cell count to 2 * 3
        g.add(r1, t.get_row(2), c15)
        assert g.num_cells == 2 * 3

        # create a second group
        g2 = Group(t)
        g2.add(c15)
        assert g2.num_cells == t.num_rows

        # add second group to original; since 2 cells overlap, total
        # should now be 2 * 2 + t_num_rows
        g.add(g2)
        assert g.num_cells == 2 * 2 + t.num_rows

        # remove c15 from g; count should remain the same
        g.remove(c15)
        assert g.num_cells == 2 * 2 + t.num_rows

        # add a cell in c5
        r200c5 = t.get_cell(r200, c5)
        assert r200c5
        g.add(r200c5)
        assert g.num_cells == 2 * 2 + t.num_rows + 1

        # remove r1 and r2 from group, individual cell now a part of component column
        g.remove(r1, t.get_row(2))
        assert g.num_cells == 3 * t.num_rows

        # remove c5, r200c5 not relevant
        g.remove(c5)
        assert g.num_cells == 2 * t.num_rows + 1

        # remove explicit rows and columns and groups; only r200c5 should remain
        g.remove(c5, c10, c15, g2)
        assert g.num_cells == 1

        # remaining element should be r200c5
        for cell in g.cells:
            cell == r200c5
