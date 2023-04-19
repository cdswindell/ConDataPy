from __future__ import annotations

from collections.abc import Collection, Iterator
from typing import cast, Optional, TYPE_CHECKING
from weakref import ref

from ..utils import ArrayList

from ..exceptions import InvalidException
from ..exceptions import UnsupportedException
from ..events import BlockedRequestException

from . import ElementType, Access, Property
from . import TableSliceElement
from . import Cell

from ..utils import JustInTimeSet

if TYPE_CHECKING:
    from .filters import FilteredColumn
    from . import Table
    from . import Row
    from ..mixins import Derivable


class _ColumnCellIterator:
    def __init__(self, col: Column) -> None:
        self._index = 0
        self._col = col
        self._table_ref = ref(col.table) if col else None
        self._num_rows = self.table.num_rows if self.table else 0

    def __iter__(self) -> Iterator[Cell]:
        self._index = 0
        self._num_rows = self.table.num_rows if self.table else 0
        return self

    def __next__(self) -> Cell:
        if self._index < self._num_rows:
            self._index += 1
            row: Row = self.table._get_slice(  # type: ignore[assignment]
                ElementType.Row, self.table._rows, Access.ByIndex, True, False, self._index
            )
            return self._col._get_cell(row, True, False)  # type: ignore[return-value]
        else:
            raise StopIteration

    @property
    def table(self) -> Table:
        return self._table_ref() if self._table_ref else None  # type: ignore[return-value]

    @property
    def column(self) -> Column:
        return self._col


# noinspection DuplicatedCode_
class Column(TableSliceElement):
    def __init__(self, te: Table, proxy: Optional[Column] = None) -> None:
        from .filters import FilteredColumn
        from .filters import FilteredTable

        # if parent row is specified, te must be a FilteredTable
        if proxy and te and not isinstance(te, FilteredTable):
            raise UnsupportedException(self, "FilteredTable Required")

        super().__init__(te)
        self._datatype: type | None = None
        self._proxy: Column | None = proxy
        self._filters = JustInTimeSet[FilteredColumn]()

        for p in self.element_type.initializable_properties():
            value = self._get_template(te).get_property(p)
            self._initialize_property(p, value)

        ci = te.row_capacity_incr if te else None
        self.__cells = ArrayList[Cell](capacity_increment=ci)  # type: ignore[arg-type]

        if proxy and isinstance(self, FilteredColumn):
            proxy.register_filter(self)

    def _delete(self, compress: bool = True) -> None:
        from .filters import FilteredColumn

        if self.is_invalid:
            return
        try:
            super()._delete(compress)
        except BlockedRequestException:
            return

        # for filter rows, deregister from parent
        if self._proxy and isinstance(self, FilteredColumn):
            self._proxy.deregister_filter(self)

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
        try:
            if self.table:
                with self.table.lock:
                    # sanity checks
                    if self.table._columns is None:
                        raise InvalidException(self, "Parent table has no columns...")
                    index = self.index - 1
                    if index < 0 or index >= self.table.num_columns:
                        raise InvalidException(self, f"Column index outside of parent Table bounds: {index}")

                    # remove this column from any groups it's in
                    self._remove_from_all_groups()

                    # clear the derivation from this column and any elements that reference self
                    self.clear_derivation()
                    self.clear_time_series()
                    self._clear_affects()

                    # invalidate cells
                    if self.__cells:
                        for cell in self.__cells:
                            if cell:
                                cell._delete()

                    # remove the column from the cols array and move all others up
                    self.table._columns.__delitem__(index)

                    # and reindex remaining columns
                    if index < self.table.num_columns:
                        for c in self.table._columns[index:]:
                            if c:
                                c._set_index(c.index - 1)

                    # clear element from current cell stack
                    self.table._purge_current_stack(self)

                    # clear current column if this one
                    if self.table.current_column == self:
                        self.table.current_column = None

                    if bool(compress):
                        self.table._reclaim_column_space()
        finally:
            self._set_index(-1)
            self._set_is_in_use(False)
            self.__cells.clear()
            self._invalidate()

    @property
    def _cells(self) -> ArrayList[Cell]:
        return self.__cells

    def register_filter(self, filter_col: FilteredColumn) -> None:
        self._filters.add(filter_col)

    def deregister_filter(self, filter_col: FilteredColumn) -> None:
        self._filters.discard(filter_col)

    def _reclaim_cell_space(self, rows: ArrayList[Row], num_rows: int) -> None:
        if 0 < num_rows < self._num_cells and self._num_cells:
            cells = ArrayList[Cell](
                initial_capacity=num_rows,
                capacity_increment=self.table.row_capacity_incr if self.table else None,  # type: ignore[arg-type]
            )
            for row in rows:
                if row and row._cell_offset >= 0:
                    cells.append(self.__cells[row._cell_offset])
            self.__cells = cells
        elif self.__cells.capacity > self._num_cells:
            self.__cells.trim()
        else:
            self.__cells = ArrayList[Cell](
                capacity_increment=self.table.row_capacity_incr if self.table else None  # type: ignore[arg-type]
            )

    @property
    def element_type(self) -> ElementType:
        return ElementType.Column

    @property
    def num_slices(self) -> int:
        return self.table.num_rows if self.table else 0

    @property
    def slices_type(self) -> ElementType:
        return ElementType.Row

    @property
    def _cells_capacity(self) -> int:
        return self.__cells.capacity

    @property
    def datatype(self) -> type | None:
        return self._datatype

    @datatype.setter
    def datatype(self, datatype: type | None) -> None:
        # datatype is stored on column for efficiency, as well as
        # in the props dict to aid in retrieval (BaseElement._find)
        self._datatype = datatype
        self._set_property(Property.DataType, datatype)

    def get_cell(self, row: Row) -> Cell | None:
        return self._get_cell(row, create_if_sparse=True, set_to_current=True)

    def _get_cell(self, row: Row, create_if_sparse: bool = True, set_to_current: bool = True) -> Cell | None:
        self.vet_components(row)
        c = None
        if self.table:
            with self.lock:  # lock this column
                num_cells = self._num_cells
                cell_offset = row._cell_offset
                if cell_offset < 0:
                    if bool(create_if_sparse):
                        with self.table.lock:
                            cell_offset = self.table._next_cell_offset
                            if cell_offset < 0:
                                raise InvalidException(self, f"Invalid cell offset returned: {cell_offset}")
                            row._set_cell_offset(cell_offset)
                    else:
                        return None
                # if the offset is equal or greater than num_cells,
                # we have to create the new cell and add it to the cell list
                if cell_offset < num_cells:
                    c = self.__cells[cell_offset]
                    if c is None and bool(create_if_sparse):  # type: ignore[unreachable]
                        c = self._create_new_cell(row)  # type: ignore[unreachable]
                        self.__cells[cell_offset] = c  # type: ignore[unreachable]
                else:
                    if bool(create_if_sparse):
                        # if cell_offset is equal to or > num_cells, this should be a new slot
                        # in which case, cell_offset should equal num_cells
                        if num_cells > cell_offset:
                            raise InvalidException(self, f"Invalid cell offset: {cell_offset} ({num_cells})")
                        # ensure capacity
                        self._cells.ensure_capacity(cell_offset + 1)
                        c = self._create_new_cell(row)
                        self.__cells[cell_offset] = c
        if c:
            if bool(set_to_current):
                self.mark_current()
                row.mark_current()
            self._set_is_in_use(True)
            row._set_is_in_use(True)
        return c

    def _create_new_cell(self, row: Row) -> Cell:
        return Cell(self, row._cell_offset)

    @property
    def _num_cells(self) -> int:
        """
        Returns the size of self.__cells, including null cells
        Returns 0 if self.__cells has not been created
        :return:
        """
        return len(self.__cells)

    def _invalidate_cell(self, cell_offset: int) -> None:
        if self.__cells is not None and cell_offset < self._num_cells:
            cell = self.__cells[cell_offset]
            if cell:
                cell._invalidate_cell()
            self.__cells[cell_offset] = cast(Cell, None)

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table.is_column_labels_indexed) if self.table else False

    @property
    def num_cells(self) -> int:
        """
        Returns the number of non-null cells in the column
        :return:
        """
        return sum(1 for c in self._cells if c) if self._cells else 0

    @property
    def cells(self) -> Iterator[Cell]:
        return _ColumnCellIterator(self)

    def mark_current(self) -> Column | None:
        self.vet_element()
        return self.table.mark_current(self) if self.table else None

    @property
    def is_null(self) -> bool:
        return self.num_cells == 0

    @property
    def derived_elements(self) -> Collection[Derivable]:
        return list()
