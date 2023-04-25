from __future__ import annotations

import pytest

from ..test_base import TestBase

from cdspy.elements import Table, Access, Group
from cdspy.elements import Column
from cdspy.exceptions import InvalidException


# noinspection PyMethodMayBeStatic,PyTypeChecker
class TestColumns(TestBase):
    def test_column_properties(self) -> None:
        c = Column(None)  # type: ignore[arg-type]
        assert c.is_initializing
        assert not c.is_initialized

        for p in c.properties:
            print(f"{p} -> {c.get_property(p)}")
            assert c.has_property(p) or c.get_property(p) is None

    def test_indexed_columns(self) -> None:
        t = Table()
        assert t

        assert not t.is_column_labels_indexed
        t.is_column_labels_indexed = True
        assert t.is_column_labels_indexed

        c1 = t.add_column()
        c1.label = "Unique Label 1"

        c2 = t.add_column()
        c2.label = "Unique Label 2"

        c3 = t.add_column()
        c3.label = "Unique Label 3"

        c4 = t.add_column()
        c4.label = "Unique Label 4"

        # relabeling a column to itself should be ok
        c2.label = "Unique Label 2"

        # relabeling c3 to an existing label should raise exception
        with pytest.raises(KeyError):
            c3.label = "Unique Label 2"

        # delete c2 should allow to rename
        c2.delete()
        c3.label = "Unique Label 2"

        # disable column label indexing and add a column with a dup label
        t.is_column_labels_indexed = False
        c2 = t.add_column(2)
        c2.label = "Unique Label 2"
        assert t.num_columns == 4
        assert c2.index == 2
        assert c4.index == 4

        # try to reindex columns; should fail
        with pytest.raises(KeyError):
            t.is_column_labels_indexed = True

        # clear dup label and retry; should succeed
        c3.label = None
        t.is_column_labels_indexed = True

        # try column retrievals by label
        assert c1 == t.get_column(Access.ByLabel, "Unique Label 1")
        assert c2 == t.get_column(Access.ByLabel, "Unique Label 2")
        assert t.get_column(Access.ByLabel, "Unique Label 3") is None
        assert c4 == t.get_column(Access.ByLabel, "Unique Label 4")

    def test_add_columns(self) -> None:
        t = Table(10, 10)
        assert t

        assert t.num_columns == 0
        c1 = t.add_column(Access.Next)
        assert c1
        assert c1.table == t
        assert c1.index == 1
        assert t.current_column == c1
        assert t.num_columns == 1

        c4 = t.add_column(Access.ByIndex, 3)
        assert c4
        assert c4.index == 3
        assert t.current_column == c4
        assert t.num_columns == 3

        c3 = t.add_column(Access.ByIndex, 3)
        assert c3
        assert c3.index == 3
        assert t.current_column == c3
        assert t.num_columns == 4
        assert t.num_rows == 0

        assert c4
        assert c4.index == 4
        assert t.current_column != c4

        c2 = t.get_column(2)
        assert c2
        assert c2.index == 2
        assert t.current_column == c2
        assert t.num_columns == 4

        assert c1.index == 1
        assert c2.index == 2
        assert c3.index == 3
        assert c4.index == 4

        c = t.get_column(Access.First)
        assert c
        assert c.index == 1

        c = t.get_column(Access.Next)
        assert c
        assert c.index == 2

        c = t.get_column(Access.Next)
        assert c
        assert c.index == 3

        c = t.get_column(Access.Next)
        assert c
        assert c.index == 4

        c = t.get_column(Access.Next)
        assert c is None
        assert t.current_column == c4  # type: ignore[unreachable]

        assert t.get_column(Access.Current) == c4

        c = t.get_column(Access.Last)
        assert c
        assert t.current_column == c4
        assert t.get_column(Access.Current) == c4

        c20 = t.add_column(20)
        assert c20
        assert c20.index == 20
        assert t.current_column == c20
        assert t.num_columns == 20

        c19 = t.get_column(Access.Previous)
        assert c19
        assert c19.index == 19
        assert t.current_column == c19
        assert t.num_columns == 20

        c19a = t.get_column(Access.Current)
        assert c19a
        assert c19a.index == 19
        assert c19a == c19
        assert t.current_column == c19 == c19a
        assert t.num_columns == 20

        c = t.get_column(Access.Previous)
        assert c
        assert c.index == 18
        assert t.current_column == c
        assert t.num_columns == 20

        c = t.get_column(Access.Last)
        assert c
        assert c.index == 20
        assert c == c20
        assert t.current_column == c == c20
        assert t.num_columns == 20

        c21 = t.add_column(Access.Last)
        assert c21
        assert c21.index == 21
        assert t.current_column == c21
        assert t.num_columns == 21
        assert t.num_rows == 0

    def test_delete_column(self) -> None:
        t = Table(10, 10)
        assert t

        g = Group(t)
        assert g

        assert t.num_columns == 0

        c1 = t.add_column(Access.Next)
        assert c1
        assert c1.index == 1
        assert c1.num_groups == 0
        assert t.num_columns == 1

        g.add(c1)
        assert g.num_columns == 1
        assert c1.num_groups == 1

        c2 = t.add_column(Access.Next)
        assert c2
        assert c2.index == 2
        assert c2.num_groups == 0
        assert t.num_columns == 2

        g.add(c2)
        assert g.num_columns == 2
        assert c2.num_groups == 1

        assert c2 in g
        assert c1 in g

        c1.delete()
        assert c1.is_invalid
        assert not c1.is_in_use
        assert c1.num_groups == 0
        assert c2
        assert c2.index == 1
        assert t.num_columns == 1
        assert g.num_columns == 1

        assert c1 not in g
        assert c2 in g

        c2.delete()
        assert c2.is_invalid
        assert t.num_columns == 0
        assert g.num_columns == 0

    def test_column_fill(self) -> None:
        t = Table(10, 10)
        assert t

        assert t.num_columns == 0
        c1 = t.add_column(16)
        assert c1
        assert c1.index == 16
        assert t.num_columns == 16
        assert t.num_cells == 0

        r1 = t.add_row()
        assert r1
        assert r1.index == 1
        assert t.num_cells == 0

        c1.fill(42)
        assert t.num_cells == 1
        assert t.get_cell_value(r1, c1) == 42

        c1.fill(412)
        assert t.get_cell_value(r1, c1) == 412

        c1.clear()
        assert t.get_cell_value(r1, c1) is None
        assert t.num_cells == 1

        r1.fill(64)
        assert t.num_cells == 16
        for idx in range(1, 17):
            assert t.get_cell_value(r1, t.get_column(idx)) == 64

        r100 = t.add_row(100)
        c1.fill("abcd")
        c_cnt = 0
        for cell in c1.cells:
            assert cell
            assert cell.value == "abcd"
            c_cnt += 1
        assert c_cnt == t.num_rows == 100

        r223 = t.add_row(223)
        t.fill(123)
        c_cnt = 0
        for cell in c1.cells:
            assert cell
            assert cell.value == 123
            c_cnt += 1
        assert c_cnt == t.num_rows == 223

        # delete some random rows
        t.delete(t.get_row(5), t.get_row(100), t.get_row(1), t.get_row(49))
        c_cnt = 0
        for cell in c1.cells:
            assert cell
            assert cell.value == 123
            c_cnt += 1
        assert c_cnt == t.num_rows == 223 - 4

        # add the new rows back, they should be empty
        t.add_row(1)
        t.add_row(5)
        t.add_row(49)
        t.add_row(100)

        for rx in [1, 5, 49, 100]:
            cell = t.get_cell(t.get_row(rx), c1)
            assert cell
            assert cell.value is None

        c_cnt = 0
        for cell in c1.cells:
            assert cell
            assert cell.row
            assert cell.row.index
            if cell.row.index not in [1, 5, 49, 100]:
                assert cell.value == 123
                c_cnt += 1
        assert c_cnt == t.num_rows - 4 == 223 - 4

        t.fill(100)
        for cell in c1._cells._list[0 : t.num_rows]:
            assert cell
            assert cell.value == 100

        c1.fill(200)
        for cell in c1._cells._list[0 : t.num_rows]:
            assert cell
            assert cell.value == 200

        assert c1.num_cells == t.num_rows == c1._num_cells
        assert len(c1._cells._list) % t.column_capacity_incr == 0
        assert c1.capacity % t.column_capacity_incr == 0

    def test_column_iterable(self) -> None:
        t = Table(10, 10)
        assert t
        assert t.num_columns == 0

        c100 = t.add_column(100)
        assert c100 is not None
        assert t.num_columns == 100

        idx = 0
        for c in t.columns:
            idx += 1
            assert c
            assert c.index == idx
        assert idx == 100

    def test_add_columns_by_value(self) -> None:
        t = Table(10, 10)
        assert t
        assert t.num_columns == 0

        # ByIndex, 2 ways
        c = t.add_column(12)
        assert c
        assert c.index == 12
        c.delete()
        assert c.is_invalid
        assert t.get_column(12) is None

        c = t.add_column(Access.ByIndex, 1)
        assert c
        assert c.index == 1

        # ByLabel
        t = Table()
        c = t.add_column(Access.ByLabel, "Test Col")
        assert c
        assert c.index == 1
        assert c.label == "Test Col"
        assert c == t.get_column(1)
        assert c == t.get_column(Access.Last)
        assert c == t.get_column(Access.First)
        assert c == t.get_column(Access.Current)
        assert c == t.get_column(Access.ByIndex, 1)

        # should fail
        with pytest.raises(InvalidException, match="Column with ByLabel 'Test Col' exists"):
            c = t.add_column(Access.ByLabel, "Test Col")

        # should succeed
        c2 = t.add_column(Access.ByLabel, "Test Col", True)
        assert c2
        assert c2.index == 2
        assert c != c2

        # add by DataType
        c = t.add_column(Access.ByDataType, Table)
        assert c
        assert c.index == 3
        assert c.datatype == Table == type(t)
        assert c == t.get_column(Access.ByDataType, Table)

        # adding a second column of type Datatype should fail
        with pytest.raises(InvalidException, match="Column with ByDataType 'Table' exists"):
            c = t.add_column(Access.ByDataType, Table)

        # this should succeed
        c = t.add_column(Access.ByDataType, Table, True)
        assert c
        assert c.index == 4

        # retrieve it again, should be first instance
        c = t.get_column(Access.ByDataType, Table)
        assert c
        assert c.index == 3

        # UUID
        cu = c.uuid
        assert cu
        assert c == t.get_column(Access.ByUUID, cu)
        assert c.index == 3

        # this should fail
        with pytest.raises(InvalidException):
            c = t.add_column(Access.ByUUID, cu)

        # this should also fail, as UUIDs must be unique
        with pytest.raises(InvalidException):
            c = t.add_column(Access.ByUUID, cu, True)

        # ByDescription
        c = t.add_column(Access.ByDescription, "Col Desc")
        assert c
        assert c.index == 5
        assert c == t.get_column(Access.Last)
        assert c == t.get_column(Access.ByDescription, "Col Desc")
        assert c.description == "Col Desc"

        # should fail
        with pytest.raises(InvalidException):
            c = t.add_column(Access.ByDescription, "Col Desc")

        # should succeed
        c = t.add_column(Access.ByDescription, "Col Desc", True)
        assert c.index == 6
