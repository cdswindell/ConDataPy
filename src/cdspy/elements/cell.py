from __future__ import annotations

from typing import Any, Iterator, Optional, TYPE_CHECKING, Collection

from threading import RLock

from . import TableContext
from . import ElementType
from .base_element import _BaseElementIterable
from . import BaseElementState
from . import TableElement

from ..mixins import Derivable

if TYPE_CHECKING:
    from . import T
    from . import Table


class Cell(TableElement, Derivable):
    __slots__ = (
        "_cell_offset",
        "_cell_value",
        "_col",
        "_state",
    )

    # TODO: make class smaller
    def __init__(self, col: TableElement, cell_offset: int) -> None:
        super().__init__(col)
        self._col = col
        self._cell_offset = cell_offset if cell_offset is not None else -1
        self._cell_value = None
        self._mark_initialized()

    def __iter__(self) -> Iterator[T]:
        return iter(_BaseElementIterable(tuple(self)))

    def _delete(self, compress: Optional[bool] = True) -> None:
        self._invalidate_cell()

    def __set_cell_value_internal(
        self, value: Any, type_safe_check: Optional[bool] = True, do_preprocess: Optional[bool] = False
    ) -> bool:
        values_differ = False
        if (value is None and self._cell_value is not None) or (value and value != self._cell_value):
            self.__setattr__("_cell_value", value)
            values_differ = True
        return values_differ

    @property
    def __cell_offset(self) -> int:
        return self.__cell_offset

    def _invalidate_cell(self) -> None:
        pass

    def _register_affects(self, d: Derivable) -> None:
        self.vet_element()
        # To minimize memory, effects are maintained in parent table
        if self.table:
            self.table._register_cell_affects(self, d)

    def _deregister_affects(self, d: Derivable) -> None:
        self.vet_element()
        # To minimize memory, effects are maintained in parent table
        if self.table:
            self.table._deregister_cell_affects(self, d)

    @property
    def lock(self) -> RLock:
        # TODO: Move out of cell class
        raise Exception("need to implement")

    @property
    def cell_value(self) -> Any:
        return self._cell_value

    @cell_value.setter
    def cell_value(self, value: Any) -> None:
        self.vet_element()
        self.__set_cell_value_internal(value)

    @property
    def element_type(self) -> ElementType:
        return ElementType.Cell

    @property
    def is_null(self) -> bool:
        return self.cell_value is None

    @property
    def num_cells(self) -> int:
        return 1

    @property
    def table(self) -> Table | None:
        return self._col.table if self._col else None

    @property
    def table_context(self) -> TableContext | None:
        self.vet_element()
        return self.table.table_context if self.table else None

    @property
    def column(self) -> TableElement | None:
        self.vet_element()
        return self._col

    @property
    def row(self) -> TableElement | None:
        self.vet_element()
        return self.table._row_by_cell_offset(self.__cell_offset) if self.table else None

    def fill(self, value: Any) -> None:
        self.cell_value = value

    def clear(self) -> None:
        return self.fill(None)

    def delete(self) -> None:
        self.__set_cell_value_internal(None, type_safe_check=False, do_preprocess=True)

    @property
    def num_groups(self) -> int:
        return 0

    @property
    def is_derived(self) -> bool:
        return self._is_set(BaseElementState.IS_DERIVED_CELL_FLAG)

    @property
    def is_pending(self) -> bool:
        return self.is_pendings

    @property
    def is_pendings(self) -> bool:
        return self._is_set(BaseElementState.IS_PENDING_FLAG)

    def __set_pendings(self, pending: bool) -> None:
        self._mutate_state(BaseElementState.IS_PENDING_FLAG, pending)

    @property
    def is_label_indexed(self) -> bool:
        return self.table.are_cell_labels_indexed if self.table else False

    @property
    def affects(self) -> Collection[Derivable]:
        return self.table._get_cell_affects(self) if self.table else []

    @property
    def derived_elements(self) -> Collection[Derivable]:
        self.vet_element()
        return tuple([self]) if self.is_derived else tuple()
