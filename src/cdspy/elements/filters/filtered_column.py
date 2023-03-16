from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from ...exceptions import UnsupportedException
from ...elements import Column

if TYPE_CHECKING:
    from . import FilteredTable


class FilteredColumn(Column):
    def __init__(self, parent_table: FilteredTable, proxy: Column) -> None:
        super().__init__(parent_table, proxy)
        self._parent = proxy
        self.is_read_only = True

    @property
    def parent(self) -> Column:
        return self._parent

    @property
    def label(self) -> str | None:
        return self.parent.label

    @label.setter
    def label(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set label of a filtered Column")

    @property
    def description(self) -> str | None:
        return self.parent.description

    @description.setter
    def description(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set description of a filtered Column")

    def fill(self, value: object) -> None:
        raise UnsupportedException(self, "Can not fill a filtered Column")

    def clear(self) -> None:
        raise UnsupportedException(self, "Can not clear a filtered Column")
