from __future__ import annotations

from ...elements import Table


class FilteredTable(Table):
    def __init__(self, parent: Table) -> None:
        super().__init__(parent)
        self._parent = parent

    @property
    def parent(self) -> Table:
        return self._parent
