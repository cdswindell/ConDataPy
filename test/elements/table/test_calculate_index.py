from __future__ import annotations

from ...test_base import TestBase
from ...test_base import PROPERTY_ABC
from ...test_base import PROPERTY_DEF

from cdspy.elements import Access
from cdspy.elements import ElementType
from cdspy.elements import TableContext
from cdspy.elements import Table


# noinspection PyMethodMayBeStatic
class TestCalculateIndex(TestBase):
    # ---------------------------
    # Method level setup/teardown
    # ---------------------------
    def setup_method(self) -> None:
        """Make sure default context is empty"""
        TableContext().clear()

    def teardown_method(self) -> None:
        """Clear default context"""
        TableContext().clear()

    def test_each_access_get_row(self) -> None:
        t: Table = Table()
        assert t is not None
        assert t.num_rows == 0
        assert t.num_columns == 0

        # test that all access methods return -1 for empty table
        for access in Access:
            assert t._calculate_index(ElementType.Row, False, access) == -1

        # add some rows
        self.add_test_rows(t)
        assert t.num_rows == len(range(0, 20))

        assert t._calculate_index(ElementType.Row, False, Access.First) == 0
        assert t._calculate_index(ElementType.Row, False, Access.Last) == 19
        assert t._calculate_index(ElementType.Row, False, Access.Current) == 19
        assert t._calculate_index(ElementType.Row, False, Access.Previous) == 18
        assert t._calculate_index(ElementType.Row, False, Access.Next) == -1

        # ByIndex, ByLabel, ByDescription, ByProperty, ByReference, ByIdent, ByUUID
        for index in range(1, 21):
            assert t._calculate_index(ElementType.Row, False, Access.ByIndex, index) == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByLabel, f"Row {index} Label") == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByReference, t._rows[index - 1]) == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByIdent, t._rows[index - 1].ident) == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByUUID, t._rows[index - 1].uuid) == index - 1
            assert (
                t._calculate_index(ElementType.Row, False, Access.ByDescription, f"Row {index} Description")
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Row, False, Access.ByProperty, PROPERTY_ABC, f"Row {index} {PROPERTY_ABC}"
                )
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Row, False, Access.ByProperty, PROPERTY_DEF, f"Row {index} {PROPERTY_DEF}"
                )
                == index - 1
            )

        index = 1
        for r in t.rows:
            assert r.index == index
            index += 1

    def test_each_access_get_column(self) -> None:
        t: Table = Table()
        assert t is not None
        assert t.num_rows == 0
        assert t.num_columns == 0

        # test that all access methods return -1 for empty table
        for access in Access:
            assert t._calculate_index(ElementType.Column, False, access) == -1

        # add some columns
        self.add_test_columns(t)
        assert t.num_columns == len(range(0, 20))

        assert t._calculate_index(ElementType.Column, False, Access.First) == 0
        assert t._calculate_index(ElementType.Column, False, Access.Last) == 19
        assert t._calculate_index(ElementType.Column, False, Access.Current) == 19
        assert t._calculate_index(ElementType.Column, False, Access.Previous) == 18
        assert t._calculate_index(ElementType.Column, False, Access.Next) == -1

        # ByIndex, ByLabel, ByDescription, ByProperty, ByReference, ByIdent, ByUUID
        for index in range(1, 21):
            assert t._calculate_index(ElementType.Column, False, Access.ByIndex, index) == index - 1
            assert t._calculate_index(ElementType.Column, False, Access.ByLabel, f"Column {index} Label") == index - 1
            assert t._calculate_index(ElementType.Column, False, Access.ByReference, t._columns[index - 1]) == index - 1
            assert (
                t._calculate_index(ElementType.Column, False, Access.ByIdent, t._columns[index - 1].ident) == index - 1
            )
            assert t._calculate_index(ElementType.Column, False, Access.ByUUID, t._columns[index - 1].uuid) == index - 1
            assert (
                t._calculate_index(ElementType.Column, False, Access.ByDescription, f"Column {index} Description")
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Column, False, Access.ByProperty, PROPERTY_ABC, f"Column {index} {PROPERTY_ABC}"
                )
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Column, False, Access.ByProperty, PROPERTY_DEF, f"Column {index} {PROPERTY_DEF}"
                )
                == index - 1
            )

        index = 1
        for c in t.columns:
            assert c.index == index
            index += 1

    def test_each_access_get_row_and_column(self) -> None:
        t: Table = Table()
        assert t is not None
        assert t.num_rows == 0
        assert t.num_columns == 0

        # test that all access methods return -1 for empty table
        for access in Access:
            assert t._calculate_index(ElementType.Row, False, access) == -1
            assert t._calculate_index(ElementType.Column, False, access) == -1

        # add some rows and columns
        self.add_test_rows(t)
        assert t.num_rows == len(range(0, 20))
        self.add_test_columns(t)
        assert t.num_columns == len(range(0, 20))

        assert t._calculate_index(ElementType.Column, False, Access.First) == 0
        assert t._calculate_index(ElementType.Column, False, Access.Last) == 19
        assert t._calculate_index(ElementType.Column, False, Access.Current) == 19
        assert t._calculate_index(ElementType.Column, False, Access.Previous) == 18
        assert t._calculate_index(ElementType.Column, False, Access.Next) == -1

        # ByIndex, ByLabel, ByDescription, ByProperty, ByReference, ByIdent, ByUUID
        for index in range(1, 21):
            assert t._calculate_index(ElementType.Column, False, Access.ByIndex, index) == index - 1
            assert t._calculate_index(ElementType.Column, False, Access.ByLabel, f"Column {index} Label") == index - 1
            assert t._calculate_index(ElementType.Column, False, Access.ByReference, t._columns[index - 1]) == index - 1
            assert (
                t._calculate_index(ElementType.Column, False, Access.ByIdent, t._columns[index - 1].ident) == index - 1
            )
            assert t._calculate_index(ElementType.Column, False, Access.ByUUID, t._columns[index - 1].uuid) == index - 1
            assert (
                t._calculate_index(ElementType.Column, False, Access.ByDescription, f"Column {index} Description")
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Column, False, Access.ByProperty, PROPERTY_ABC, f"Column {index} {PROPERTY_ABC}"
                )
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Column, False, Access.ByProperty, PROPERTY_DEF, f"Column {index} {PROPERTY_DEF}"
                )
                == index - 1
            )

        index = 1
        for c in t.columns:
            assert c.index == index
            index += 1

        assert t._calculate_index(ElementType.Row, False, Access.First) == 0
        assert t._calculate_index(ElementType.Row, False, Access.Last) == 19
        assert t._calculate_index(ElementType.Row, False, Access.Current) == 19
        assert t._calculate_index(ElementType.Row, False, Access.Previous) == 18
        assert t._calculate_index(ElementType.Row, False, Access.Next) == -1

        # ByIndex, ByLabel, ByDescription, ByProperty, ByReference, ByIdent, ByUUID
        for index in range(1, 21):
            assert t._calculate_index(ElementType.Row, False, Access.ByIndex, index) == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByLabel, f"Row {index} Label") == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByReference, t._rows[index - 1]) == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByIdent, t._rows[index - 1].ident) == index - 1
            assert t._calculate_index(ElementType.Row, False, Access.ByUUID, t._rows[index - 1].uuid) == index - 1
            assert (
                t._calculate_index(ElementType.Row, False, Access.ByDescription, f"Row {index} Description")
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Row, False, Access.ByProperty, PROPERTY_ABC, f"Row {index} {PROPERTY_ABC}"
                )
                == index - 1
            )
            assert (
                t._calculate_index(
                    ElementType.Row, False, Access.ByProperty, PROPERTY_DEF, f"Row {index} {PROPERTY_DEF}"
                )
                == index - 1
            )

        index = 1
        for r in t.rows:
            assert r.index == index
            index += 1
