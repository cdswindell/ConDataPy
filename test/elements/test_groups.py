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

        g = None
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
            r1g.clear()

        with pytest.raises(TypeError):
            r1g[0] = None

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
