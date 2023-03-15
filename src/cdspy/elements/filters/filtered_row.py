from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from ...exceptions import UnsupportedException
from ...elements import Row

if TYPE_CHECKING:
    from . import FilteredTable


class FilteredRow(Row):
    def __init__(self, parent_table: FilteredTable, parent_row: Row) -> None:
        super().__init__(parent_table, parent_row)
        self._parent = parent_row

    @property
    def parent(self) -> Row:
        return self._parent

    @property
    def label(self) -> str | None:
        return self.parent.label

    @label.setter
    def label(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set label of a filtered Row")

    @property
    def description(self) -> str | None:
        return self.parent.description

    @description.setter
    def description(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set description of a filtered Row")

    def fill(self, value: object) -> None:
        raise UnsupportedException(self, "Can not fill a filtered Row")

    def clear(self) -> None:
        raise UnsupportedException(self, "Can not clear a filtered Row")
