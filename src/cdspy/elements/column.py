from __future__ import annotations

from collections.abc import Collection
from typing import Any, List, Optional, TYPE_CHECKING

from ..exceptions import InvalidException
from ..exceptions import UnsupportedException
from ..events import BlockedRequestException

from . import ElementType
from . import TableSliceElement

from ..utils import JustInTimeSet

if TYPE_CHECKING:
    from .filters import FilteredColumn
    from . import TableElement
    from . import Cell
    from . import Row
    from ..mixins import Derivable


# noinspection DuplicatedCode
class Column(TableSliceElement):
    def __init__(self, te: TableElement, proxy: Optional[Column] = None) -> None:
        from .filters import FilteredColumn
        from .filters import FilteredTable

        # if parent row is specified, te must be a FilteredTable
        if proxy and te and not isinstance(te, FilteredTable):
            raise UnsupportedException(self, "FilteredTable Required")

        super().__init__(te)
        self.__cells = None
        self.__cells_capacity = 0
        self._datatype = None
        self._proxy: Column | None = proxy
        self._filters = JustInTimeSet[FilteredColumn]()

        for p in self.element_type.initializable_properties():
            value = self._get_template(te).get_property(p)
            self._initialize_property(p, value)

        if proxy and isinstance(self, FilteredColumn):
            proxy.register_filter(self)

        self._mark_initialized()

    def _delete(self, compress: Optional[bool] = True) -> None:
        from .filters import FilteredColumn

        try:
            super()._delete(compress)
        except BlockedRequestException:
            return

        # for filter rows, deregister from parent
        if self._proxy and isinstance(self, FilteredColumn):
            self._proxy.deregister(self)

        # delete all filter columns based on this (self)
        while self._filters:
            fr = self._filters.pop()
            fr.delete()

        # for good measure
        self._filters.clear()

        # clean up remote handlers
        if self._remote_uuids:
            self._clear_remote_uuids()

        # remove column from parent table
        table = self.table
        if table:
            with table.lock:
                # sanity checks
                cols = table._cols
                if cols is None:
                    raise InvalidException(self, "Parent table has no columns...")
                idx = self.index - 1
                if idx < 0 or idx >= len(cols):
                    raise InvalidException(self, f"Column index outside of parent Table bounds: {idx}")
                # remove this column from any groups it's in
                self._remove_from_all_groups()
                # clear the derivation from this column and any elements that reference self
                self._clear_derivation()
                self._clear_affects()

                # invalidate cells
                if self.__cells:
                    for cell in self.__cells:
                        cell._invalidate_cell()

                # remove the column from the cols array and move all others up
                del cols[idx]

                # and reindex remaining columns
                if idx < table.num_columns:
                    for c in cols[idx:]:
                        if c is not None:
                            c._set_index(c.index - 1)

                # clear element from current cell stack
                table.purge_current_stack(self)

                if bool(compress):
                    table._reclaim_column_space()

        self.__cells_capacity = 0
        self._set_index(-1)
        self._set_is_in_use(False)

        self.__cells = None
        self.invalidate()

    @property
    def _cells_capacity(self) -> int:
        return self.__cells_capacity

    @property
    def _num_cells(self) -> int:
        """
        Returns the size of self.__cells, including null cells
        Returns 0 if self.__cells has not been created
        :return:
        """
        return len(self.__cells) if self.__cells else 0

    def _reclaim_cell_space(self, rows: Collection[Row], num_rows: int) -> None:
        if num_rows > 0 and self._num_cells:
            num_cells = self._num_cells
            if num_cells > num_rows:
                cells = [None] * num_cells
                idx = 0
                for row in rows:
                    if row and row._cell_offset > 0:
                        cells[idx] = self.__cells[row._cell_offset]
                        idx += 1
                self.__cells = cells
                num_cells = self._num_cells
            elif self.__cells_capacity > num_cells:
                # trim unused space
                self.__cells = self.__cells[0:num_cells]
            self.__cells_capacity = num_cells
        else:
            self.__cells = None
            self.__cells_capacity = 0

    @property
    def _cells(self) -> Collection[Cell] | None:
        return self.__cells

    def _create_new_cell(self, row: Row) -> Cell:
        return Cell(self, row._cell_offset)

    def _invalidate_cell(self, cell_offset: int) -> None:
        if self._num_cells and cell_offset < self._num_cells:
            cell = self.__cells[cell_offset]
            if cell:
                cell._invalidate_cell()

    def _ensure_cell_capacity(self, num_required: int) -> List[Any] | None:
        if self.table.num_rows > 0:
            req_capacity = self.table._calculate_rows_capacity(num_required)
            if self.__cells is None:
                self.__cells = [None] * req_capacity
            elif req_capacity > self._cells_capacity:
                self.__cells.extend([None] * (req_capacity - self._cells_capacity))
            self.__cells_capacity = req_capacity
        return self.__cells

    def _get_cell(
        self, row: Row, set_to_current: Optional[bool] = True, create_if_sparse: Optional[bool] = True
    ) -> Cell | None:
        self.vet_components(row)
        c = None
        with self.lock:  # lock this column
            num_cells = self._num_cells
            cell_offset = row._cell_offset
            if cell_offset < 0:
                if bool(create_if_sparse):
                    with self.table.lock:
                        cell_offset = self.table._calculate_next_available_cell_offset()
                        if cell_offset < 0:
                            raise InvalidException(self, f"Invalid cell offset returned: {cell_offset}")
                        row._set_cell_offset(cell_offset)
                else:
                    return None

            # if the offset is equal or greater than num_cells,
            # we have to create the new cell and add it to the cell list,
            if cell_offset < num_cells:
                c = self.__cells[cell_offset]
                if c is None and bool(create_if_sparse):
                    c = self._create_new_cell(row)
                    self.__cells[cell_offset] = c
            else:
                if bool(create_if_sparse):
                    # if cell_offset is equal to or > num_cells, this should be a new slot
                    # in which case, cell_offset should equal num_cells
                    if num_cells < cell_offset:
                        raise InvalidException(self, f"Invalid cell offset: {cell_offset} ({num_cells})")

                    # ensure capacity
                    self._ensure_cell_capacity(cell_offset + 1)

                    # and expand the cell array
                    self.__cells.expand([None] * (cell_offset - num_cells))
                    num_cells = cell_offset

                    c = self._create_new_cell(row)
                    self.__cells[cell_offset] = c
        if c is not None:
            if bool(set_to_current):
                self.mark_current()
                row.mark_current()
            self._set_is_in_use(True)
            row._set_is_in_use(True)
        return c

    def get_cell(self, row: Row) -> Cell | None:
        return self._get_cell(row, set_to_current=True, create_if_sparse=True)

    def register_filter(self, filter_col: FilteredColumn) -> None:
        self._filters.add(filter_col)

    def deregister_filter(self, filter_col: FilteredColumn) -> None:
        self._filters.discard(filter_col)

    @property
    def element_type(self) -> ElementType:
        return ElementType.Column

    @property
    def datatype(self) -> type | None:
        return self._datatype

    @datatype.setter
    def datatype(self, datatype: type | None) -> None:
        self._datatype = type

    @property
    def num_cells(self) -> int:
        """
        Returns the number of non-null cells in the column
        :return:
        """
        return sum(1 for c in self._cells if c is not None) if self._cells else 0

    @property
    def is_null(self) -> bool:
        return self.num_cells == 0

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table.is_column_labels_indexed) if self.table else False

    @property
    def derived_elements(self) -> Collection[Derivable]:
        return list()

    @property
    def num_slices(self) -> int:
        return self.table.num_rows if self.table else 0

    @property
    def slices_type(self) -> ElementType:
        return ElementType.Row

    def mark_current(self) -> Column:
        self.vet_element()
        return self.table.mark_current(self) if self.table else None
