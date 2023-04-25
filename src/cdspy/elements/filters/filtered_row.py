from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Any

from .. import BaseElement
from ...exceptions import UnsupportedException
from ...elements import Row

if TYPE_CHECKING:
    from . import FilteredTable


class FilteredRow(Row):
    def __init__(self, parent_table: FilteredTable, proxy: Row) -> None:
        super().__init__(parent_table, proxy)
        self._parent = proxy

    @property
    def parent(self) -> Row:
        return self._parent

    @BaseElement.label.getter
    def label(self) -> str:
        return self.parent.label

    @BaseElement.label.setter
    def label(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set label of a filtered Row")

    @BaseElement.description.getter
    def description(self) -> str | None:
        return self.parent.description

    @BaseElement.description.setter
    def description(self, value: Optional[str]) -> None:
        raise UnsupportedException(self, "Can not set description of a filtered Row")

    def fill(self, o: Any, preprocess: Optional[bool] = True) -> None:
        raise UnsupportedException(self, "Can not fill a filtered Row")

    def clear(self) -> None:
        raise UnsupportedException(self, "Can not clear a filtered Row")
