from __future__ import annotations

from typing import Any, cast, Iterator, Type, TYPE_CHECKING, Collection

from threading import RLock


from . import TableContext
from . import ElementType
from .base_element import _BaseElementIterable
from . import BaseElementState
from . import TableElement

from ..exceptions import ReadOnlyException

from ..computation import Token

from ..mixins import Derivable

if TYPE_CHECKING:
    from . import T
    from . import Table
    from . import Row
    from . import Column


class Cell(TableElement, Derivable):
    __slots__ = (
        "_offset",
        "_value",
        "_col",
        "_lock",
        "_state",
    )

    # TODO: make class smaller
    def __init__(self, col: Column, cell_offset: int) -> None:
        super().__init__(col)
        self._col = col
        self._offset = cell_offset if cell_offset is not None else -1
        self._value = None
        self._lock = RLock()  # TODO: move to table
        # initialize special properties
        row = self.table._row_by_cell_offset(self._offset) if self.table else None
        self.is_read_only = (col.is_write_protected if col else False) or (row.is_write_protected if row else False)
        self.is_supports_null = (col.is_nulls_supported if col else False) and (
            row.is_nulls_supported if row else False
        )
        self._mark_initialized()

    def __iter__(self) -> Iterator[T]:
        return _BaseElementIterable[Cell](tuple(self))

    def _delete(self, compress: bool = True) -> None:
        self._invalidate_cell()

    def __set_cell_value_internal(self, value: Any, type_safe_check: bool = True, preprocess: bool = False) -> bool:
        self.__decrement_pendings()
        if bool(type_safe_check) and value is not None and self.is_datatype_enforced:
            if self.is_datatype_mismatch(value):
                raise ValueError(
                    f"Datatype Mismatch: Expected: '{cast(Type, self._enforced_datatype).__name__}', rejected: '{type(value).__name__}'"
                )
        values_differ = False
        if (value is None and self._value is not None) or (value and value != self._value):
            if bool(preprocess):
                pass
            self.__setattr__("_value", value)
            values_differ = True
        return values_differ

    @property
    def _enforced_datatype(self) -> Type | None:
        if self.column is not None and self.column.datatype:
            return self.column.datatype
        if self._value is not None:
            return type(self._value)
        return None

    def is_datatype_mismatch(self, value: Any) -> bool:
        if value is not None:
            col_type = self.column.datatype if self.column else None
            if col_type and not isinstance(value, col_type):
                return True
            if self.value and not isinstance(value, type(self.value)):
                return True
        return False

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
        return self._lock

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, value: Any) -> None:
        from . import Property

        self.vet_element()
        if value is not None and isinstance(value, Token):
            self._post_result(value)
        else:
            if self.is_write_protected:
                raise ReadOnlyException(self, Property.CellValue)
            if value is None and not self.is_nulls_supported:
                raise ValueError("Cell value can not be set to None")

            self.clear_derivation()
            self._reset(BaseElementState.IS_AWAITING_FLAG)

            old_value = self._value
            differ = self.__set_cell_value_internal(value, type_safe_check=True, preprocess=True)

            if differ:
                pass

    @property
    def formatted_value(self) -> Any:
        # TODO: apply format
        return self._value

    @property
    def element_type(self) -> ElementType:
        return ElementType.Cell

    @property
    def is_null(self) -> bool:
        return self.value is None

    @property
    def is_write_protected(self) -> bool:
        cwp = self.column.is_write_protected if self.column else False
        rwp = self.row.is_write_protected if self.row else False
        return self.is_read_only or cwp or rwp

    @property
    def is_nulls_supported(self) -> bool:
        csn = self.column.is_nulls_supported if self.column else False
        rsn = self.row.is_nulls_supported if self.row else False
        return self.is_supports_null and csn and rsn

    @property
    def is_datatype_enforced(self) -> bool:
        if self.table and self.table.is_datatype_enforced:
            return True
        if self.column and self.column.is_datatype_enforced:
            return True
        if self.row and self.row.is_datatype_enforced:
            return True
        return self.is_enforce_datatype

    @property
    def num_cells(self) -> int:
        return 1

    @property
    def table(self) -> Table:
        return self._col.table if self._col else None  # type: ignore[return-value]

    @property
    def table_context(self) -> TableContext:
        self.vet_element()
        return self.table.table_context if self.table else None  # type: ignore[return-value]

    @property
    def column(self) -> Column:
        self.vet_element()
        return self._col

    @property
    def row(self) -> Row:
        self.vet_element()
        return self.table._row_by_cell_offset(self._offset) if self.table else None  # type: ignore[return-value]

    def fill(self, value: Any) -> None:
        self.value = value

    def clear(self) -> None:
        return self.fill(None)

    def delete(self) -> None:
        self.__set_cell_value_internal(None, type_safe_check=False, preprocess=True)

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

    def __decrement_pendings(self) -> None:
        # TODO: implement
        pass

    def _post_result(self, t: Token) -> bool:
        # TODO: implement
        return False

    @property
    def affects(self) -> Collection[Derivable]:
        return self.table._get_cell_affects(self) if self.table else []

    @property
    def derived_elements(self) -> Collection[Derivable]:
        self.vet_element()
        return tuple([self]) if self.is_derived else tuple()

    def clear_derivation(self) -> None:
        pass
