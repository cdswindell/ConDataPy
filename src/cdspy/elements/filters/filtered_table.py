from __future__ import annotations

from typing import Optional, cast, TYPE_CHECKING

from .. import Table, Group, TableContext, Access

if TYPE_CHECKING:
    from .. import TableElement, Row, Column, Cell


class FilteredTable(Table):
    @classmethod
    def create_table(cls, t: Table, g: Group, tc: Optional[TableContext] = None) -> FilteredTable:
        tc = t.table_context if tc is None else tc
        return cls(t, g, tc)

    def __init__(self, parent: Table, scope: Group, context: Optional[TableContext] = None) -> None:
        from . import FilteredColumn, FilteredRow

        context = parent.table_context if context is None else context
        super().__init__(parent)

        self._parent = parent
        self.parent._register_filter(self)

        # add the columns and rows defined in the scope

        if scope.num_columns != scope._num_effective_columns:
            parent._ensure_columns_exist()
        for col in scope._effective_columns:
            fc = FilteredColumn(self, col)
            self._insert_slice(fc, Access.Next, False, False)
        if scope.num_rows != scope._num_effective_rows:
            parent._ensure_rows_exist()
        for row in scope._effective_rows:
            fr = FilteredRow(self, row)
            self._insert_slice(fr, Access.Next, False, False)
        self._mark_initialized()

    def delete(self, *elems: TableElement) -> None:
        with self.lock:
            if self.is_invalid:
                return
            if self.parent:
                self.parent._deregister_filter(self)

            super().delete(*elems)
            self._parent = cast(Table, None)

    @property
    def parent(self) -> Table:
        return self._parent

    def _get_parent_cell(self, pt: Table, pr: Row, pc: Column, create_if_sparse: bool, set_current: bool) -> Cell:
        return super()._get_table_cell(pt, pr, pc, create_if_sparse, set_current)

    def _get_cell(
        self, row: Row, col: Column, create_if_sparse: bool = True, set_to_current: bool = True
    ) -> Cell | None:
        from . import FilteredColumn, FilteredRow, FilteredCell

        if isinstance(row, FilteredRow) and isinstance(col, FilteredColumn):
            parent_cell = self._get_table_cell(self.parent, row.parent, col.parent, create_if_sparse, False)
            return FilteredCell(row, col, parent_cell)
        else:
            return self.parent._get_cell(
                row.parent if isinstance(row, FilteredRow) else row,
                col.parent if isinstance(col, FilteredColumn) else col,
                create_if_sparse,
                set_to_current,
            )
