from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Any, cast

from .. import Row, Column, Property

from ...exceptions import UnsupportedException, ReadOnlyException
from ...elements import Cell

if TYPE_CHECKING:
    from . import FilteredRow, FilteredColumn, FilteredTable


class FilteredCell(Cell):
    __slots__ = ["_filter_row", "_parent"]

    def __init__(self, row: FilteredRow, col: FilteredColumn, cell: Cell) -> None:
        super().__init__(col, -1)
        self._filter_row = row
        self._parent = cell
        self.is_read_only = True

    @property
    def _row(self) -> Row:
        return self._filter_row

    @property
    def row(self) -> Row:
        return self._filter_row

    @property
    def column(self) -> Column:
        return self._col

    @property
    def table(self) -> FilteredTable:
        return cast(FilteredTable, self._col.table if self._col else None)

    @Cell.value.getter
    def value(self) -> Any:
        return self._parent.value

    @Cell.value.setter
    def value(self, value: Any) -> None:
        raise ReadOnlyException(self, Property.CellValue)

    @property
    def is_write_protected(self) -> bool:
        return True

    def fill(self, o: Any, preprocess: Optional[bool] = True) -> None:
        raise ReadOnlyException(self, Property.CellValue)

    def clear(self) -> None:
        raise ReadOnlyException(self, Property.CellValue)

    def delete(self) -> None:
        raise UnsupportedException(self, "Can not delete a filtered cell")
