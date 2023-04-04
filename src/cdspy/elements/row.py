from __future__ import annotations

from collections.abc import Collection
from typing import Optional, TYPE_CHECKING

from ..exceptions import InvalidException
from ..exceptions import UnsupportedException
from ..events import BlockedRequestException

from . import ElementType
from . import TableSliceElement

from ..utils import JustInTimeSet
from ..mixins import Derivable

if TYPE_CHECKING:
    from . import Table
    from . import Column
    from . import Cell
    from .filters import FilteredRow


# noinspection DuplicatedCode
class Row(TableSliceElement):
    def __init__(self, te: Table, parent_row: Optional[Row] = None) -> None:
        from .filters import FilteredRow
        from .filters import FilteredTable

        # if parent row is specified, te must be a FilteredTable
        if parent_row and te and not isinstance(te, FilteredTable):
            raise UnsupportedException(self, "FilteredTable Required")

        super().__init__(te)
        self.__cell_offset = -1
        self._proxy: Row | None = parent_row
        self._filters = JustInTimeSet[FilteredRow]()

        # initialize properties
        for p in self.element_type.initializable_properties():
            value = self._get_template(te).get_property(p)
            self._initialize_property(p, value)

        if parent_row and isinstance(self, FilteredRow):
            parent_row.register_filter(self)
        # don't mark as intialized; Rows should only be created by methods in Table

    def _delete(self, compress: bool = True) -> None:
        from .filters import FilteredRow

        if self.is_invalid:
            return
        try:
            super()._delete(compress)
        except BlockedRequestException:
            return

        # for filter rows, deregister from parent
        if self._proxy and isinstance(self, FilteredRow):
            self._proxy.deregister(self)

        # delete all filter rows based on this (self)
        while self._filters:
            fr = self._filters.pop()
            if fr:
                fr.delete()

        # for good measure
        self._filters.clear()

        # clean up remote handlers
        if self._remote_uuids:
            self._clear_remote_uuids()

        # remove row from parent table
        try:
            if self.table:
                with self.table.lock:
                    index = self.index - 1
                    if index < 0 or index >= self.table.num_rows:
                        raise InvalidException(self, f"Row index {index+1} outside of parent table")
                    self._remove_from_all_groups()

                    # clear the derivation from this column and any elements that reference self
                    self.clear_derivation()
                    self.clear_time_series()
                    self._clear_affects()

                    # remove the row from the table
                    if self.table._rows[index] != self:
                        raise InvalidException(self, "Invalid state: removed row doesn't equal itself")
                    self.table._rows.__delitem__(index)

                    # reindex the rows after the one removed
                    if index < self.table.num_rows:
                        for r in self.table._rows[index:]:
                            if r:
                                r._set_index(r.index - 1)

                    # cache the cell offset so that it can be reused
                    self.table._cache_cell_offset(self._cell_offset)

                    # clear this element from the current cell stack
                    self.table._purge_current_stack(self)

                    # clear current row if this one
                    if self.table.current_row == self:
                        self.table.current_row = None

                    if bool(compress):
                        self.table._reclaim_row_space()
        finally:
            self._set_cell_offset(-1)
            self._set_index(-1)
            self._set_is_in_use(False)
            self._invalidate()

    @property
    def _cell_offset(self) -> int:
        return self.__cell_offset

    def _set_cell_offset(self, offset: int) -> None:
        self.__cell_offset = offset
        if offset >= 0 and self.table:
            self.table._map_cell_offset_to_row(self)

    def _get_cell(self, col: Column, set_to_current: bool = True, create_if_sparse: bool = True) -> Cell | None:
        self.vet_components(col)
        return col._get_cell(self, set_to_current, create_if_sparse)

    def get_cell(self, col: Column) -> Cell | None:
        return self._get_cell(col, set_to_current=True, create_if_sparse=True)

    def register_filter(self, filtered: FilteredRow) -> None:
        self._filters.add(filtered)

    def deregister_filter(self, filtered: FilteredRow) -> None:
        self._filters.discard(filtered)

    @property
    def element_type(self) -> ElementType:
        return ElementType.Row

    @property
    def num_slices(self) -> int:
        return self.table.num_columns if self.table else 0

    @property
    def slices_type(self) -> ElementType:
        return ElementType.Column

    def mark_current(self) -> Row | None:
        self.vet_element()
        return self.table.mark_current(self) if self.table else None

    @property
    def num_cells(self) -> int:
        self.vet_element()
        if self._cell_offset < 0:
            return 0
        if self.table is None:
            raise InvalidException(self, "Row must belong to a Table")
        num_cells = 0
        if self.table._columns:
            for col in self.table._columns:
                if col and self._cell_offset < col._num_cells:
                    if col._get_cell(self, False, False):
                        num_cells += 1
        return num_cells

    @property
    def _num_cells(self) -> int:
        return self.table.num_columns if self.table else 0

    @property
    def is_null(self) -> bool:
        return self.num_cells == 0

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table.is_row_labels_indexed) if self.table else False

    @property
    def derived_elements(self) -> Collection[Derivable]:
        return []
