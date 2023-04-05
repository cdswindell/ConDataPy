from __future__ import annotations

from typing import Any, cast, Collection, Dict, Iterator, List
from typing import Optional, Set, Type, TYPE_CHECKING

from threading import RLock

from . import TableContext
from . import ElementType
from . import Property
from . import EventType
from .base_element import _BaseElementIterable
from . import BaseElementState
from . import TableElement

from ..exceptions import ReadOnlyException

from ..computation import Token
from ..templates import TableCellValidator
from ..templates import TableEventListener

from ..events import BlockedRequestException

from ..mixins import Derivable

if TYPE_CHECKING:
    from . import T
    from . import Table
    from . import Row
    from . import Column
    from . import Group


class Cell(TableElement, Derivable):
    __slots__ = [
        "_offset",
        "_value",
        "_col",
        "_lock",
    ]

    # TODO: make class smaller
    def __init__(self, col: Column, cell_offset: int) -> None:
        super().__init__(col)
        self._col = col
        self._offset = cell_offset if cell_offset is not None else -1
        self._value = None
        self._lock = RLock()
        # initialize special properties
        row = self.table._row_by_cell_offset(self._offset) if self.table else None
        self.is_read_only = (col.is_write_protected if col else False) or (row.is_write_protected if row else False)
        self.is_supports_null = (col.is_nulls_supported if col else False) and (
            row.is_nulls_supported if row else False
        )
        self._mark_initialized()

    def __del__(self) -> None:
        print(f"Deleting {self.element_type.name}...")
        if self.is_valid:
            self._delete()
            print(f"Deleted {self.element_type.name} (via __del__)...")

    def __iter__(self) -> Iterator[T]:
        return _BaseElementIterable[Cell](tuple(self))

    def __str__(self) -> str:
        return str(self.formatted_value)

    def _delete(self, compress: bool = True) -> None:
        self._invalidate_cell()

    def _invalidate_cell(self) -> None:
        self.label = None  # if cells are indexed, we need to remove from map
        self.clear_derivation()

        # remove all listeners
        self.remove_all_listeners()

        # clear derivations on elements dependent on this cell
        affects = self.table._get_cell_affects(self, False) if self.table else []
        for affected in affects:
            affected.clear_derivation()

        # remove cell from any groups
        for g in self._get_groups():
            if g and g.is_valid:
                g.remove(self)

        self.__decrement_pendings()

        # set column cell slot to None
        if self._col and self._offset >= 0 and self._col._cells[self._offset] == self:
            # noinspection PyTypeChecker
            self._col._cells[self._offset] = cast(Cell, None)

        # reset the cell state
        self._value = None
        self._col = None  # type: ignore[assignment]
        self._offset = -1

        # and invalidate, marking it as deleted
        self._invalidate()

    def recalculate(self) -> None:
        self.vet_element()
        derivation = self.table._get_cell_derivation(self) if self.table else None
        if derivation:
            derivation.recalculate_target()
            self._fire_events(EventType.OnRecalculate)

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

    def _reset_element_properties(self) -> None:
        if self.table:
            self.table._reset_cell_element_properties(self)

    def _element_properties(self, create_if_empty: bool = False) -> Optional[Dict]:
        """
        Overridden in Cell class, as Property array is maintained in parent table
        to minimize space consumed by table cells
        :param create_if_empty:
        :return:
        """
        return self.table._get_cell_element_properties(self, create_if_empty) if self.table else None

    def _apply_transform(self, value: Any) -> Any:
        validator = self.validator
        if validator is None and self.column:
            validator = self.column.cell_validator
        if validator is None and self.row:
            validator = self.row.cell_validator

        if validator:
            value = validator.transform(value)
        return value

    def _fire_events(self, evt: EventType, *args: Any) -> None:
        if self.table:
            self.table._fire_cell_events(self, evt, *args)

    @property
    def listeners(self) -> List[TableEventListener]:
        if self.table:
            return self.table._get_cell_listeners(self)
        else:
            return []

    def add_listeners(self, et: EventType, *listeners: TableEventListener) -> bool:
        if self.table:
            return self.table._add_cell_listeners(self, et, *listeners)
        else:
            return False

    def remove_listeners(self, et: EventType, *listeners: TableEventListener) -> bool:
        if self.table:
            return self.table._remove_cell_listeners(self, et, *listeners)
        else:
            return False

    @property
    def has_listeners(self) -> bool:
        if self.table:
            return self.table._has_cell_listeners(self)
        else:
            return False

    def remove_all_listeners(self, *events: EventType) -> List[TableEventListener]:
        if self.table:
            return self.table._remove_all_cell_listeners(self, *events)
        else:
            return []

    def __set_cell_value_internal(self, value: Any, type_safe_check: bool = True, preprocess: bool = False) -> bool:
        self.__decrement_pendings()
        if bool(type_safe_check) and value is not None and self.is_datatype_enforced:
            if self.is_datatype_mismatch(value):
                datatype = cast(Type, self.enforced_datatype).__name__
                raise ValueError(f"Datatype Mismatch: Expected: '{datatype}', rejected: '{type(value).__name__}'")
        values_differ = False
        if (value is None and self._value is not None) or (value and value != self._value):
            if bool(preprocess):
                value = self._apply_transform(value)
            try:
                self._fire_events(EventType.OnBeforeNewValue, self._value, value)
            except BlockedRequestException:
                return False
            self.__setattr__("_value", value)
            values_differ = True
        return values_differ

    def _get_groups(self) -> Set[Group]:
        if self.table:
            return self.table._get_cell_groups(self)
        else:
            return set()

    @property
    def lock(self) -> RLock:
        # TODO: Move out of cell class
        return self._lock

    @property
    def validator(self) -> TableCellValidator | None:
        with self.lock:
            if self._is_set(BaseElementState.HAS_CELL_VALIDATOR_FLAG):
                return cast(TableCellValidator, self.get_property(Property.CellValidator))
            else:
                return None

    @validator.setter
    def validator(self, tcv: Optional[TableCellValidator]) -> None:
        if tcv:
            self._set_property(Property.CellValidator, tcv)
        else:
            self._clear_property(Property.CellValidator)
        self._mutate_state(BaseElementState.HAS_CELL_VALIDATOR_FLAG, tcv is not None)

    @property
    def enforced_datatype(self) -> Type | None:
        if self.column and self.column.datatype:
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
                if self.table and self.table.is_automatic_recalculate_enabled:
                    # TODO: DerivationImpl.recalculateAffected(this)
                    pass
                self._fire_events(EventType.OnNewValue, old_value, self._value)

    @property
    def _display_format_str(self) -> str | None:
        for e in [self, self.column, self.row, self.table]:
            if e and e.display_format:
                return e.display_format
        return None

    @property
    def _units_str(self) -> str | None:
        for e in [self, self.column, self.row, self.table]:
            if e and e.units:
                return e.units
        return None

    @property
    def is_formatted(self) -> bool:
        return bool(self._display_format_str)

    # noinspection PyBroadException
    @property
    def formatted_value(self) -> str | None:
        if self.is_pending:
            return "Pending..."
        if self.is_awaiting:
            return "Awaiting..."
        if self._value is None:
            return None
        # try the format string
        try:
            return self._display_format_str.format(self._value, units=self._units_str)  # type: ignore[union-attr]
        except Exception:
            pass
        if isinstance(self._value, bool):
            return "Yes" if bool(self._value) else "No"
        return str(self._value)

    @property
    def element_type(self) -> ElementType:
        return ElementType.Cell

    @property
    def is_null(self) -> bool:
        return self.value is None

    @property
    def is_pending(self) -> bool:
        return self._is_set(BaseElementState.IS_PENDING_FLAG)

    def __set_pending(self, pending: bool) -> None:
        self._mutate_state(BaseElementState.IS_PENDING_FLAG, pending)

    @property
    def is_pendings(self) -> bool:
        return self.is_pending

    @property
    def is_awaiting(self) -> bool:
        return self._is_set(BaseElementState.IS_AWAITING_FLAG)

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
        for e in [self.table, self.column, self.row]:
            if e and e.is_datatype_enforced:
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
        self._reset_element_properties()

    @property
    def num_groups(self) -> int:
        return 0

    @property
    def is_derived(self) -> bool:
        return self._is_set(BaseElementState.IS_DERIVED_CELL_FLAG)

    @property
    def is_label_indexed(self) -> bool:
        return self.table.are_cell_labels_indexed if self.table else False

    def __decrement_pendings(self) -> None:
        if self.is_pendings:
            self.__set_pending(False)
            if self.table:
                self.table._decrement_pendings()
            if self.column:
                self.column._decrement_pendings()
            if self.row:
                self.row._decrement_pendings()

    def _post_result(self, t: Token) -> bool:
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
