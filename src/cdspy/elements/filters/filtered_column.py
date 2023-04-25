from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Any, cast

from .filtered_cell import FilteredCell
from .. import BaseElement, Row, Cell
from ...exceptions import UnsupportedException

from ...elements import Column

if TYPE_CHECKING:
    from . import FilteredTable, FilteredRow


class FilteredColumn(Column):
    def __init__(self, parent_table: FilteredTable, proxy: Column) -> None:
        super().__init__(parent_table, proxy)
        self._parent = proxy
        self.is_read_only = True

    @property
    def parent(self) -> Column:
        return self._parent

    def _get_cell(self, row: Row, create_if_sparse: bool = True, set_to_current: bool = True) -> Cell | None:
        if isinstance(row, FilteredRow):
            ftable = cast(FilteredTable, self.table)
            parent_cell = ftable._get_parent_cell(ftable.parent, row.parent, self.parent, create_if_sparse, False)
            return FilteredCell(row, self, parent_cell)
        else:
            return self.parent._get_cell(row, create_if_sparse, set_to_current)

    @BaseElement.label.getter
    def label(self) -> str:
        return self.parent.label

    @BaseElement.label.setter
    def label(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set label of a filtered Column")

    @BaseElement.description.getter
    def description(self) -> str | None:
        return self.parent.description

    @BaseElement.description.setter
    def description(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set description of a filtered Column")

    @Column.datatype.getter
    def datatype(self) -> type | None:
        return self.parent.datatype

    @Column.datatype.setter
    def datatype(self, datatype: type | None) -> None:
        raise UnsupportedException(self, "Can not set the datatype of a filtered Column")

    def fill(self, o: Any, preprocess: Optional[bool] = True) -> None:
        raise UnsupportedException(self, "Can not fill a filtered Column")

    def clear(self) -> None:
        raise UnsupportedException(self, "Can not clear a filtered Column")
