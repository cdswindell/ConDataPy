from __future__ import annotations

import threading
import weakref

from typing import Any, Final, Optional, overload, Collection, TYPE_CHECKING

from ..events import BlockedRequestException

from . import BaseElementState
from . import BaseElement
from . import ElementType
from . import TableElement
from . import TableCellsElement
from . import TableContext

from ..computation import recalculate_affected

from ..mixins import Derivable

if TYPE_CHECKING:
    from . import Row
    from . import Column
    from . import Cell

CURRENT_CELL_KEY: Final = "_cr"


class _CellReference:
    def __init__(self, cr: _CellReference | None) -> None:
        self._row = cr.current_row if cr else None
        self._col = cr.current_column if cr else None

    def set_current_cell_reference(self, table: Table) -> None:
        cr = table._current_cell
        cr.current_row = self.current_row
        cr.current_column = self.current_column

    @property
    def current_row(self) -> Row | None:
        return self._row

    @current_row.setter
    def current_row(self, row: Row) -> None:
        if row:
            row.vet_parent(row)
        self._row = row

    @property
    def current_column(self) -> Column | None:
        return self._col

    @current_column.setter
    def current_column(self, col: Column) -> None:
        if col:
            col.vet_parent(col)
        self._col = col


class Table(TableCellsElement):
    # create thread local storage, but only once for the class
    _THREAD_LOCAL_STORAGE = threading.local()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(None)

        num_rows = self._parse_args(int, "num_rows", 0, TableContext().row_capacity_incr, *args, **kwargs)
        num_cols = self._parse_args(int, "num_cols", 1, TableContext().column_capacity_incr, *args, **kwargs)
        parent_context = self._parse_args(TableContext, "parent_context", None, None, *args, **kwargs)
        template_table = self._parse_args(Table, "template_table", None, None, *args, **kwargs)

        # define Table with superclass
        self._set_table(self)

        # we need a context for default property initialization purposes
        parent_context = (
            parent_context if parent_context else template_table.table_context if template_table else TableContext()
        )
        self._context: TableContext | None = parent_context

        # finally, with context set, initialize default properties
        for p in ElementType.Table.initializable_properties():
            source = template_table if template_table else parent_context
            self._initialize_property(p, source.get_property(p))

        # Initialize other instance attributes
        self.__rows = [None] * max(num_rows, self.row_capacity_incr)
        self.__cols = [None] * max(num_cols, self.column_capacity_incr)

        self._next_row_index = 0
        self._next_column_index = 0

        self._rows_capacity = self._calculate_rows_capacity(num_rows)
        self._columns_capacity = self._calculate_columns_capacity(num_cols)

        self.__table_creation_thread = weakref.ref(threading.current_thread())

        # finally, register table with context
        parent_context._register(self)

        # and mark instance as initialized
        self._mark_initialized()

    def __del__(self) -> None:
        print(f"*** Deleting table...")
        if self.is_valid:
            self._delete(False)

    def _delete(self, compress: Optional[bool] = True) -> None:
        if self.is_invalid:
            return
        try:
            super()._delete(compress)
        except BlockedRequestException:
            return

        with self.lock:
            try:
                if compress:
                    self._reclaim_column_space()
                    self._reclaim_row_space()
                super()._delete(compress)
            finally:
                self._clear_current_cell()
                self._invalidate()
                if self.table_context:
                    self.table_context._deregister(self)
                    self._context = None

    @property
    def rows_capacity(self) -> int:
        return self._rows_capacity

    @property
    def columns_capacity(self) -> int:
        return self._columns_capacity

    @property
    def _current_cell(self) -> _CellReference:
        """
        Maintain a "current cell" independently in each thread that accesses this table
        :return:
        """
        with self.lock:
            try:
                return Table._THREAD_LOCAL_STORAGE._current_cell
            except AttributeError:
                Table._THREAD_LOCAL_STORAGE._current_cell = _CellReference()
                return Table._THREAD_LOCAL_STORAGE._current_cell

    def _clear_current_cell(self):
        with self.lock:
            try:
                del Table._THREAD_LOCAL_STORAGE._current_cell
            except AttributeError:
                pass

    @property
    def _rows(self) -> Collection[Row]:
        return self.__rows

    @property
    def _columns(self) -> Collection[Column]:
        return self.__cols

    def _calculate_rows_capacity(self, num_required: int) -> int:
        capacity = self.row_capacity_incr
        if num_required > 0:
            remainder = num_required % capacity
            capacity = num_required + (capacity - remainder if remainder > 0 else 0)
        return capacity

    def _calculate_columns_capacity(self, num_required: int) -> int:
        capacity = self.column_capacity_incr
        if num_required > 0:
            remainder = num_required % capacity
            capacity = num_required + (capacity - remainder if remainder > 0 else 0)
        return capacity

    def _reclaim_column_space(self) -> None:
        pass

    def _reclaim_row_space(self) -> None:
        pass

    def _get_cell_affects(self, cell: Cell, include_indirects: Optional[bool] = True) -> Collection[Derivable]:
        return []

    @property
    def table(self) -> Table:
        return self

    @property
    def table_context(self) -> TableContext | None:
        return self._context

    @property
    def element_type(self) -> ElementType:
        return ElementType.Table

    @property
    def is_datatype_enforced(self) -> bool:
        if self.is_enforce_datatype:
            return True
        return self.table_context.is_enforce_datatype if self.table_context else False

    @property
    def is_nulls_supported(self) -> bool:
        if self.is_supports_null:
            return True
        return self.table_context.is_supports_null if self.table_context else False

    @property
    def num_rows(self) -> int:
        return self._next_row_index

    @property
    def num_columns(self) -> int:
        return self._next_row_index

    @property
    def num_cells(self) -> int:
        return 0

    @property
    def is_null(self) -> bool:
        return not self.num_rows and not self.num_columns and not self.num_cells

    @property
    def num_groups(self) -> int:
        return 0

    @property
    def are_cell_labels_indexed(self) -> bool:
        return False

    def fill(self, o: Optional[object]) -> None:
        pass

    def clear(self) -> None:
        self.fill(None)

    @property
    def is_label_indexed(self) -> bool:
        return False

    @property
    def derived_elements(self) -> Collection[Derivable]:
        return []

    # override to
    @BaseElement.is_persistent.setter  # type: ignore
    def is_persistent(self, state: bool) -> None:
        self._mutate_state(BaseElementState.IS_TABLE_PERSISTENT_FLAG, state)  # type: ignore
        if self.is_initialized and self.table_context:
            self.table_context._register(self)

    def delete(self, *elems: TableElement) -> None:
        if elems:
            deleted_any = False
            for elem in elems:
                if elem and elem.is_valid and elem.table == self:
                    elem._delete(False)
                    del elem
                    deleted_any = True

            if deleted_any:
                self._reclaim_column_space()
                self._reclaim_row_space()
                recalculate_affected(self)
        else:
            # delete the entire table
            self._delete(True)

    @property
    def current_row(self) -> Row | None:
        self._current_cell.current_row

    @current_row.setter
    def current_row(self, row: Row | None) -> None:
        self._current_cell.current_row = row

    @property
    def current_column(self) -> Column | None:
        self._current_cell.current_column

    @current_column.setter
    def current_column(self, col: Column | None) -> None:
        self._current_cell.current_column = col

    @overload
    def mark_current(self, elem: Row) -> Row | None:
        pass

    @overload
    def mark_current(self, elem: Column) -> Column | None:
        pass

    @overload
    def mark_current(self, elem: Cell) -> Cell | None:
        pass

    def mark_current(self, new_current: Row | Column | Cell) -> Row | Column | Cell | None:
        prev = None
        if isinstance(new_current, Column):
            prev = self._current_cell.current_column
            self._current_cell.current_column = new_current
        elif isinstance(new_current, Row):
            prev = self._current_cell.current_row
            self._current_cell.current_row = new_current


        return prev
