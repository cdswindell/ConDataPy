from __future__ import annotations

from typing import cast, Optional, Collection

from . import BaseElement
from . import ElementType
from . import TableCellsElement
from . import TableContext
from ..mixins import Derivable


class Table(TableCellsElement):
    def __init__(
        self,
        num_rows: Optional[int] = TableContext().row_capacity_incr,
        num_cols: Optional[int] = TableContext().column_capacity_incr,
        table_context: TableContext = TableContext(),
        template_table: Optional[Table] = None,
    ) -> None:
        super().__init__(None)

        # we need a context for default property initialization purposes
        table_context = table_context if table_context else TableContext()

        # initialize default properties
        for p in ElementType.Table.initializable_properties():
            source = template_table if template_table else table_context
            self._initialize_property(p, source.get_property(p))

        # define Table with superclass
        self._set_table(self)

        # register table with table_context
        self._context = table_context._register(self)

    @property
    def table(self) -> Table:
        return self

    @property
    def table_context(self) -> TableContext:
        return self._context

    @property
    def element_type(self) -> ElementType:
        return ElementType.Table

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
        BaseElement.is_persistent.fset(self, state)  # type: ignore
        self.table_context._register(self)
