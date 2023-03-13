from __future__ import annotations

from typing import Optional, Collection, TYPE_CHECKING

from . import BaseElementState
from . import BaseElement
from . import ElementType
from . import TableElement
from . import TableCellsElement
from . import TableContext

from ..computation import recalculate_affected

from ..mixins import Derivable

if TYPE_CHECKING:
    from . import Cell


class Table(TableCellsElement):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(None)

        num_rows = self._parse_args(int, 'num_rows', 0, TableContext().row_capacity_incr, *args, **kwargs)
        num_cols = self._parse_args(int, 'num_cols', 1, TableContext().column_capacity_incr, *args, **kwargs)
        table_context = self._parse_args(TableContext, 'table_context', None, None, *args, **kwargs)
        template_table = self._parse_args(Table, 'template_table', None, None, *args, **kwargs)

        # define Table with superclass
        self._set_table(self)

        # we need a context for default property initialization purposes
        table_context = table_context if table_context else template_table.table_context if template_table else TableContext()
        self._context = table_context

        # finally, with context set, initialize default properties
        for p in ElementType.Table.initializable_properties():
            source = template_table if template_table else table_context
            self._initialize_property(p, source.get_property(p))

        # if num_rows or num_cols were specified, apply now
        if num_rows:
            self.row_capacity_incr = num_rows
        if num_cols:
            self.column_capacity_incr = num_cols

        # finally, register table with context
        table_context._register(self)

    def __del__(self) -> None:
        print(f"*** Deleting {self}")
        self._delete(False)

    def _delete(self, compress: Optional[bool] = True) -> None:
        with self.lock:
            try:
                if compress:
                    self._reclaim_column_space()
                    self._reclaim_row_space()
                super()._delete(compress)
            finally:
                self._invalidate()
                if self.table_context:
                    self.table_context._deregister(self)
                    self._context = None

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
        return 0

    @property
    def num_columns(self) -> int:
        return 0

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

    def fill(self, o: Optional[object]) -> bool:
        return True

    def clear(self) -> bool:
        return True

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
        if self.table_context:
            self.table_context._register(self)

    def delete(self, *elems: TableElement) -> None:
        if elems:
            deleted_any = False
            for elem in elems:
                if elem and elem.is_valid and elem.table == self:
                    elem._delete(False)
                    deleted_any = True

            if deleted_any:
                self._reclaim_column_space()
                self._reclaim_row_space()
                recalculate_affected(self)
        else:
            # delete the entire table
            self._delete(True)
