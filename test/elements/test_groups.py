from __future__ import annotations

import gc
import pytest
import re

from ..test_base import TestBase

from cdspy.elements import Table, Group, Access, Property
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

    def test_group_persistence(self) -> None:
        t = Table()
        assert t

        g = Group(t)
        assert g
        assert not g.is_persistent
        assert t.num_groups == 1

        # dereference group; it should be deleted on gc
        g = None  # type: ignore[assignment]
        gc.collect()
        assert t.num_groups == 0

        # recreate group, but mark it persistent
        g = Group(t)
        assert g
        g.is_persistent = True
        assert t.num_groups == 1

        # dereference group; it should remain in table
        g = None  # type: ignore[assignment]
        gc.collect()
        assert t.num_groups == 1

    def test_group_labels(self) -> None:
        t = Table()
        assert t
        assert not t.is_group_labels_indexed
        t.is_group_labels_indexed = True
        assert t.is_group_labels_indexed

        # create a group with a label
        g1 = Group(t, "abc")
        assert g1
        assert g1.label == "abc"
        assert g1.is_label_indexed

        # make sure we cant add another group with the same label
        with pytest.raises(KeyError):
            g2 = Group(t, "abc")

        # remove index
        t.is_group_labels_indexed = False
        assert not t.is_group_labels_indexed
        assert not g1.is_label_indexed

        # recreate group with same label
        g2 = Group(t, "abc")  # type: ignore[unreachable]
        assert g2
        assert g2.label == "abc"
        assert not g2.is_label_indexed
        assert g2 != g1

        # should not be able to reindex group labels given duplicate
        with pytest.raises(KeyError):
            t.is_group_labels_indexed = True

        # relabel g2 and retry
        g2.label = "def"
        assert not g2.is_label_indexed
        t.is_group_labels_indexed = True
        assert g2.is_label_indexed

        # verify index
        assert "abc" in t._group_label_index
        assert g1 == t._group_label_index["abc"]
        assert "def" in t._group_label_index
        assert g2 == t._group_label_index["def"]

        # verify index adapts to changes
        g1.label = "xyz"
        assert "abc" not in t._group_label_index
        assert "xyz" in t._group_label_index
        assert g1 == t._group_label_index["xyz"]

        g2.label = None
        assert "def" not in t._group_label_index

        # dereference g1 should remove value from index
        g1 = None
        gc.collect()
        assert "xyz" not in t._group_label_index
        assert "abc" not in t._group_label_index
        assert len(t._group_label_index) == 0

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

    # noinspection PyStatementEffect
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

    def test_group_fill(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g = Group(t)
        g.add(c2)

        for cell in c2.cells:
            assert cell.value is None

        g.fill(42)
        for cell in c2.cells:
            assert cell.value == 42

        for cell in g.cells:
            assert cell.value == 42
            assert cell.column == c2

        g.clear()
        for cell in c2.cells:
            assert cell.value is None

        g.add(r200)
        g.fill(33)
        for cell in c2.cells:
            if cell.row == r200:
                assert cell.value == 33
            else:
                assert cell.value is None

        g.remove(r200)
        g.add(r1)
        g.fill("abcd")
        for cell in c2.cells:
            if cell.row == r1:
                assert cell.value == "abcd"
            elif cell.row == r200:
                assert cell.value == 33
            else:
                assert cell.value is None

        for cell in g.cells:
            assert cell.value == "abcd"

    def test_group_equals(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g = Group(t)
        g.add(c2)

        g2 = Group(t)
        g2.add(c2)
        assert g.equal(g2)

        g.add(r1)
        assert not g.equal(g2)

    def test_group_and(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g = Group(t)
        g.add(c2)

        g2 = Group(t)
        g2.add(c2)
        assert g.equal(g2)

        g3 = g & g2
        assert g3.equal(g)
        assert g3.equal(g2)

        g.add(r1, r200)
        g3 = g & g2
        assert g3.equal(g)
        assert not g3.equal(g2)

    def test_group_iand(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g = Group(t)
        g.add(c2)

        g2 = Group(t)
        g2.add(c2)
        assert g.equal(g2)

        g &= g2
        assert g2.equal(g)

        # we have to reconstruct g, as and operation converts group from
        # row/column-based to cell-based
        g = Group(t)
        g.add(c2)
        g.add(r1, r200)
        assert g.num_cells == 2

        g2.add(r1, r200)
        assert g2.num_cells == 2

        g2.remove(r200)
        assert g2.num_cells == 1

        g &= g2
        assert g.num_cells == 1

        for cell in g.cells:
            assert cell == t.get_cell(r1, c2)

    def test_group_responds_to_table_changes(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g = Group(t)
        g.add(c2)
        assert g.num_cells == 200

        # add a new row, group should expand
        r201 = t.add_row(201)
        assert g.num_cells == 201

        # change group to explicit rows
        g.add(r1, r200, r201)
        assert g.num_cells == 3

        # delete r1, group should shrink
        t.delete(r1)
        assert g.num_cells == 2

        # try this with a cell
        r1c1 = t.get_cell(t.get_row(1), c1)
        assert r1c1
        g.add(r1c1)
        assert g.num_cells == 3

        # delete c1; group should shrink
        t.delete(c1)
        assert c1.is_invalid
        assert g.num_cells == 2

    def test_copy_group(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g = Group(t)
        g.add(c2)
        assert g.num_cells == 200

        r1c1 = t.get_cell(t.get_row(1), c1)
        assert r1c1
        g.add(r1c1)
        assert g.num_cells == 201

        # make a copy
        g1 = g.copy()
        assert g1
        assert len(g1) == len(g)
        assert g1.equal(g)

        for r in g.rows:
            assert r in g1
        for c in g.columns:
            assert c in g1
        for cg in g.columns:
            assert cg in g1
        for cc in g._cells:
            assert cc in g1
        for cc in g.cells:
            assert cc in g1

        for r in g1.rows:
            assert r in g
        for c in g1.columns:
            assert c in g
        for cg in g1.columns:
            assert cg in g
        for cc in g1._cells:
            assert cc in g
        for cc in g1.cells:
            assert cc in g

    def test_group_union(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g = Group(t, None, c2)
        assert g
        assert c2 in g
        assert len(g) == t.num_rows

        g2 = Group(t, None, t.get_cell(r1, c1), t.get_cell(r200, c1))
        assert g2
        assert t.get_cell(r1, c1) in g2
        assert t.get_cell(r200, c1) in g2
        assert g2.num_cells == 2

        # perform logical "or" on g and g2
        g3 = g | g2
        assert g3
        assert len(g3) == len(g) + len(g2)

        # delete a few rows from the table, all groups should contract
        t.delete(t.get_row(100), t.get_row(101))
        assert len(g3) == len(g) + len(g2)
        assert len(g) == t.num_rows == 198
        assert len(g2) == 2

        # use union method
        g3 = g.union(g2)
        assert g3
        assert len(g3) == len(g) + len(g2) == 200

        # update in place (__ior__)
        g |= g2
        assert g
        assert len(g) == t.num_rows + len(g2)

        # delete col 1, verify g2 has no cells
        t.delete(c1)
        assert len(g) == t.num_rows
        assert len(g2) == 0

    def test_group_intersection(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g1 = Group(t, None, c2)
        assert g1
        assert c2 in g1
        assert r1 not in g1
        assert r200 not in g1
        assert t.get_cell(r1, c2) in g1
        assert t.get_cell(r200, c2) in g1
        assert len(g1) == t.num_rows

        g2 = Group(t, None, r1, r200)
        assert g2
        assert c2 not in g2
        assert r1 in g2
        assert r200 in g2
        assert t.get_cell(r1, c2) in g2
        assert t.get_cell(r200, c2) in g2
        assert len(g2) == 2 * t.num_columns

        # test intersection (via __and__)
        g3 = g1 & g2
        assert g3
        assert len(g3) == 2
        assert t.get_cell(r1, c2) in g3
        assert t.get_cell(r200, c2) in g3

        # delete row 100, nothing should change in g3
        t.delete(t.get_row(100))
        assert g3
        assert len(g3) == 2
        assert t.get_cell(r1, c2) in g3
        assert t.get_cell(r200, c2) in g3

        # use named operator
        g4 = g1.intersection(g2)
        assert g4
        assert g4.equal(g3)
        assert len(g4) == 2
        assert t.get_cell(r1, c2) in g4
        assert t.get_cell(r200, c2) in g4

        # update in place (__iand__)
        g1 &= g2
        assert g1
        assert g1.equal(g3)
        assert g1.equal(g4)
        assert len(g1) == 2
        assert t.get_cell(r1, c2) in g1
        assert t.get_cell(r200, c2) in g1
        assert c2 not in g1

    def test_group_difference(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g1 = Group(t, None, c2)
        assert g1
        assert c2 in g1
        assert r1 not in g1
        assert r200 not in g1
        assert t.get_cell(r1, c2) in g1
        assert t.get_cell(r200, c2) in g1
        assert len(g1) == t.num_rows

        g2 = Group(t, None, r1, r200)
        assert g2
        assert c2 not in g2
        assert r1 in g2
        assert r200 in g2
        assert t.get_cell(r1, c2) in g2
        assert t.get_cell(r200, c2) in g2
        assert len(g2) == 2 * t.num_columns

        # test difference (via __sub__)
        g3 = g1 - g2
        assert g3
        assert t.get_cell(r1, c2) not in g3
        assert t.get_cell(r200, c2) not in g3
        assert g3.num_cells == t.num_rows - 2

        # test difference (via named method)
        g4 = g1.difference(g2)
        assert g4
        assert g4.equal(g3)
        assert t.get_cell(r1, c2) not in g4
        assert t.get_cell(r200, c2) not in g4
        assert g4.num_cells == t.num_rows - 2

        # test difference in place (via __isub__)
        g1 -= g2
        assert g1
        assert g1.equal(g3)
        assert t.get_cell(r1, c2) not in g1
        assert t.get_cell(r200, c2) not in g1
        assert g1.num_cells == t.num_rows - 2

        # retest with different groups
        g1 = Group(t, None, r1, r200)
        assert t.get_cell(r1, c2) in g1
        assert t.get_cell(r200, c2) in g1
        assert len(g1) == 2 * t.num_columns

        g2 = Group(t, None, c2)
        assert t.get_cell(r1, c2) in g2
        assert t.get_cell(r200, c2) in g2
        assert len(g2) == t.num_rows

        # test difference (via __sub__)
        g3 = g1 - g2
        assert g3
        assert t.get_cell(r1, c2) not in g3
        assert t.get_cell(r200, c2) not in g3
        assert g3.num_cells == t.num_columns

        # test difference (via __sub__)
        g3 = g2 - g1
        assert g3
        assert t.get_cell(r1, c2) not in g3
        assert t.get_cell(r200, c2) not in g3
        assert g3.num_cells == t.num_rows - 2

    def test_group_symmetric_difference(self) -> None:
        t = Table(1000, 1000)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g1 = Group(t, None, c2)
        assert g1
        assert c2 in g1
        assert r1 not in g1
        assert r200 not in g1
        assert t.get_cell(r1, c2) in g1
        assert t.get_cell(r200, c2) in g1
        assert len(g1) == t.num_rows

        g2 = Group(t, None, r1, r200)
        assert g2
        assert c2 not in g2
        assert r1 in g2
        assert r200 in g2
        assert t.get_cell(r1, c2) in g2
        assert t.get_cell(r200, c2) in g2
        assert len(g2) == 2 * t.num_columns

        # test difference (via __xor__)
        g3 = g1 ^ g2
        assert g3
        assert t.get_cell(r1, c1) in g3
        assert t.get_cell(r200, c1) in g3
        assert t.get_cell(r1, c2) not in g3
        assert t.get_cell(r200, c2) not in g3
        assert g3.num_cells == t.num_rows - 2 + 2

        # test difference (via named method)
        g4 = g1.symmetric_difference(g2)
        assert g4
        assert g4.equal(g3)
        assert t.get_cell(r1, c1) in g4
        assert t.get_cell(r200, c1) in g4
        assert t.get_cell(r1, c2) not in g4
        assert t.get_cell(r200, c2) not in g4
        assert g4.num_cells == t.num_rows - 2 + 2

        # test difference in place (via __isub__)
        g1 ^= g2
        assert g1
        assert g1.equal(g3)
        assert t.get_cell(r1, c1) in g1
        assert t.get_cell(r200, c1) in g1
        assert t.get_cell(r1, c2) not in g1
        assert t.get_cell(r200, c2) not in g1
        assert g1.num_cells == t.num_rows - 2 + 2

        # retest with different groups
        g1 = Group(t, None, r1, r200)
        assert t.get_cell(r1, c1) in g1
        assert t.get_cell(r200, c1) in g1
        assert t.get_cell(r1, c2) in g1
        assert t.get_cell(r200, c2) in g1
        assert len(g1) == 2 * t.num_columns

        g2 = Group(t, None, c2)
        assert t.get_cell(r1, c1) not in g2
        assert t.get_cell(r200, c1) not in g2
        assert t.get_cell(r1, c2) in g2
        assert t.get_cell(r200, c2) in g2
        assert len(g2) == t.num_rows

        # test symmetric difference (via __xor__)
        g3 = g1 ^ g2
        assert g3
        assert t.get_cell(r1, c1) in g3
        assert t.get_cell(r200, c1) in g3
        assert t.get_cell(r1, c2) not in g3
        assert t.get_cell(r200, c2) not in g3
        assert g3.num_cells == t.num_rows - 2 + 2

        # test difference (via __sub__)
        g3 = g2 ^ g1
        assert g3
        assert t.get_cell(r1, c1) in g3
        assert t.get_cell(r200, c1) in g3
        assert t.get_cell(r1, c2) not in g3
        assert t.get_cell(r200, c2) not in g3
        assert g3.num_cells == t.num_rows - 2 + 2

    def test_group_is_disjoint(self) -> None:
        t = Table(200, 2)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g1 = Group(t, None, c1)
        g2 = Group(t, None, c2)
        assert g1.is_disjoint(g2)
        assert g2.is_disjoint(g1)

        # groups overlap, not disjoint
        g2 = Group(t, None, r1)
        assert not g1.is_disjoint(g2)
        assert not g2.is_disjoint(g1)

    def test_group_is_subset(self) -> None:
        t = Table(200, 2)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g1 = Group(t, None, c1)
        g2 = Group(t, None, t.get_cell(r1, c1), t.get_cell(r200, c1))
        assert not g1.is_subset(g2)
        assert g2.is_subset(g1)

        # groups have elements not in common
        g2 = Group(t, None, c2)
        assert not g1.is_subset(g2)
        assert not g2.is_subset(g1)

        g2.add(r1, c1)
        assert not g1.is_subset(g2)
        assert not g2.is_subset(g1)

    def test_group_is_superset(self) -> None:
        t = Table(200, 2)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        g1 = Group(t, None, c1)
        g2 = Group(t, None, t.get_cell(r1, c1), t.get_cell(r200, c1))
        assert g1.is_superset(g2)
        assert not g2.is_superset(g1)

        # groups are disjoint
        g2 = Group(t, None, c2)
        assert not g1.is_superset(g2)
        assert not g2.is_superset(g1)
        assert g2.is_disjoint(g1)
        assert g1.is_disjoint(g2)

        # groups have some elements in common and others that are not
        g2.add(r1, c1)
        assert not g1.is_superset(g2)
        assert not g2.is_superset(g1)
        assert not g2.is_disjoint(g1)
        assert not g1.is_disjoint(g2)

    def test_group_similarity(self) -> None:
        t = Table(200, 2)
        r1 = t.add_row(1)
        r200 = t.add_row(200)
        c1 = t.add_column()
        c2 = t.add_column()

        # create disjoint groups, similarity == 0
        g1 = t.add_group(c1)
        g2 = t.add_group(c2)
        assert g1.is_disjoint(g2)
        assert g1.similarity(g2) == 0.0
        assert g1.jaccard_index(g2) == 0.0

        # create minimally overlapping groups
        g1 = t.add_group(r1, r200, c1)
        g2 = t.add_group(c1)
        assert len(g1) == 2
        assert len(g2) == 200
        assert not g1.is_disjoint(g2)
        assert g1.similarity(g2) == 2.0/200

        # create overlapping groups
        g1 = t.add_group(r1, r200, c1)
        g2 = t.add_group(r1, r200)
        assert len(g1) == 2
        assert len(g2) == 4
        assert not g1.is_disjoint(g2)
        assert g1.similarity(g2) == 2.0/4.0

    def test_get_group(self) -> None:
        t1 = Table()
        t1.is_group_labels_indexed = True

        g1 = t1.add_group()
        g1.label = 'abc'
        g1.description = 'group 1'
        g1.set_property('my-prop', 'my-value-1')
        g1.tags = '123, 456'
        assert len(g1.tags) == 2

        g2 = t1.add_group()
        g2.label = 'def'
        g2.description = 'group 2'
        g2.set_property('my-prop', 'my-value-2')
        g2.tags = '456, 789'
        assert len(g2.tags) == 2

        assert g2 != g1
        assert t1.get_group(ident=g1.ident) == g1
        assert t1.get_group(label=g1.label) == g1
        assert t1.get_group(description=g1.description) == g1
        assert t1.get_group(uuid=g1.uuid) == g1
        assert t1.get_group(tags=g1.tags) == g1
        assert t1.get_group(tags='123') == g1
        assert t1.get_group(Access.ByReference, g1) == g1
        assert t1.get_group(Access.ByProperty, 'my-prop', 'my-value-1') == g1

        assert t1.get_group(ident=g2.ident) == g2
        assert t1.get_group(label=g2.label) == g2
        assert t1.get_group(description=g2.description) == g2
        assert t1.get_group(uuid=g2.get_property(Property.UUID)) is None
        assert t1.get_group(tags=g2.tags) == g2
        assert t1.get_group(tags='789') == g2
        assert t1.get_group(Access.ByReference, g2) == g2
        assert t1.get_group(Access.ByProperty, 'my-prop', 'my-value-2') == g2

        t2 = Table()
        g3 = t2.add_group()
        g3.label = 'def'
        g3.description = 'group 3'
        g3.set_property('my-prop', 'my-value-1')
        g3.tags = '123, 456'
        assert len(g1.tags) == 2

        g4 = t2.add_group()
        g4.label = 'abc'
        g4.description = 'group 4'
        g4.set_property('my-prop', 'my-value-2')
        g4.tags = '456, 789'
        assert len(g2.tags) == 2

        # cross table references return None or throw errors
        assert t2.get_group(ident=g1.ident) is None
        assert t2.get_group(label=g1.label) is g4
        assert t2.get_group(uuid=g1.uuid) is None
        assert t2.get_group(tags=g3.tags) == g3
        assert t2.get_group(description=g1.description) is None
        assert t2.get_group(Access.ByProperty, 'my-prop', 'my-value-2') == g4

        with pytest.raises(InvalidParentException, match=re.escape("Not child's parent: [Table]->[Group: abc]")):
            assert t2.get_group(Access.ByReference, g1) == g1
