from __future__ import annotations

import math
from typing import Any, Final, cast, Collection, Dict, Iterator, List, Callable, Union
from typing import Optional, Type, TYPE_CHECKING
from threading import RLock
import uuid

from . import TableContext
from . import ElementType
from . import Property
from . import EventType
from .base_element import _BaseElementIterable
from . import BaseElementState
from . import TableElement

from ..exceptions import ReadOnlyException

from ..computation import Token, Derivation, ErrorCode, ErrorResult
from ..templates import TableCellValidator, LambdaTransformer, TableCellTransformer, LambdaValidator
from ..templates import TableEventListener

from ..events import BlockedRequestException

from ..mixins import Derivable, Groupable

NaN: Final = float("NaN")
Inf: Final = float("inf")
NInf: Final = float("-inf")


if TYPE_CHECKING:
    from . import T
    from . import Table
    from . import Row
    from . import Column
    from . import Group


class Cell(TableElement, Derivable, Groupable):
    __slots__ = [
        "_offset",
        "_value",
        "_col",
        "_lock",
        "__weakref__",
    ]

    # TODO: make class smaller
    def __init__(self, col: Column, cell_offset: int) -> None:
        super().__init__(col)
        self._col = col
        self._offset = cell_offset if cell_offset is not None else -1
        self._value = None
        self._lock = RLock()
        self._set(BaseElementState.IS_PERSISTENT_FLAG)
        # initialize special properties
        for p in self.element_type.initializable_properties():
            if p in [Property.Units, Property.DisplayFormat]:
                continue
            self._initialize_property(p, self.table.get_property(p))
        self._mark_initialized()

    def __del__(self) -> None:
        if self.is_valid:
            self._delete()

    def __iter__(self) -> Iterator[T]:
        return _BaseElementIterable[Cell](tuple(self))

    def __str__(self) -> str:
        return str(self.formatted_value)

    @property
    def element_type(self) -> ElementType:
        return ElementType.Cell

    def _delete(self, compress: bool = True) -> None:
        if self.is_valid:
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
        self._remove_from_all_groups()

        self.__decrement_pendings()

        # set column cell slot to None
        if self._col and self._offset >= 0 and self._col._cells[self._offset] == self:
            # noinspection PyTypeChecker
            self._col._cells[self._offset] = cast(Cell, None)

        # reset the cell state
        self._value: Any = None
        self._col = None  # type: ignore[assignment]
        self._offset = -1

        # and invalidate, marking it as deleted
        self._reset_element_properties()
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

    def get_property(self, key: Union[Property, str, None]) -> Any:
        return super().get_property(key)

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

    def _set_cell_value_no_datatype_check(self, value: Any) -> bool:
        return self._set_cell_value_internal(value, False, False)

    def _set_cell_value_internal(self, value: Any, type_safe_check: bool = True, preprocess: bool = False) -> bool:
        with self.lock:
            if self.is_value_error:
                self._set_error_message(None)
            self.__decrement_pendings()
            if bool(type_safe_check) and value is not None and self.is_datatype_enforced:
                if self.is_datatype_mismatch(value):
                    datatype = cast(Type, self.enforced_datatype).__name__
                    raise ValueError(f"Datatype Mismatch: Expected: '{datatype}', rejected: '{type(value).__name__}'")
            values_differ = False
            if (value is None and self._value is not None) or (value is not None and value != self._value):
                if bool(preprocess):
                    value = self._apply_transform(value)
                try:
                    self._fire_events(EventType.OnBeforeNewValue, self._value, value)
                except BlockedRequestException:
                    return False
                self._value = value
                values_differ = True
            return values_differ

    @property
    def is_value_error(self) -> bool:
        self.vet_element()
        return self.error_code != ErrorCode.NoError

    @property
    def error_code(self) -> ErrorCode:
        self.vet_element()
        if self._value == NaN:
            return ErrorCode.NaN
        elif self._value == Inf or self._value == NInf:
            return ErrorCode.Infinity
        elif isinstance(self._value, ErrorCode):
            return self._value  # type: ignore[unreachable]
        elif isinstance(self._value, ErrorResult):
            return self._value.error_code  # type: ignore[unreachable]
        elif isinstance(self._value, float):
            fvalue = float(self._value)  # type: ignore[unreachable]
            if math.isinf(fvalue):
                return ErrorCode.Infinity
            elif math.isnan(fvalue):
                return ErrorCode.NaN
        return ErrorCode.NoError

    @property
    def error_message(self) -> str | None:
        if isinstance(self._value, ErrorResult):
            return self._value.error_message  # type: ignore[unreachable]
        elif self._is_set(BaseElementState.HAS_CELL_ERROR_MSG_FLAG):
            return cast(str, self.get_property(Property.ErrorMessage))
        else:
            return None

    def _set_error_message(self, emsg: Optional[str]) -> None:
        if emsg:
            self._set(BaseElementState.HAS_CELL_ERROR_MSG_FLAG)
            self._set_property(Property.ErrorMessage, emsg.strip())
        else:
            self._reset(BaseElementState.HAS_CELL_ERROR_MSG_FLAG)
            self._clear_property(Property.ErrorMessage)

    def is_datatype_mismatch(self, value: Any) -> bool:
        if value is not None:
            col_type = self.column.datatype if self.column else None
            if col_type and not isinstance(value, col_type):
                return True
            if self.value and not isinstance(value, type(self.value)):
                return True
        return False

    @property
    def enforced_datatype(self) -> Type | None:
        if self.column and self.column.datatype:
            return self.column.datatype
        if self._value is not None:
            return type(self._value)  # type: ignore[unreachable]
        return None

    @property
    def is_label_indexed(self) -> bool:
        return self.table.are_cell_labels_indexed if self.table else False

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
        try:  # type: ignore[unreachable]
            return self._display_format_str.format(self._value, self._units_str)  # type: ignore[union-attr]
        except Exception:
            pass
        if isinstance(self._value, bool):
            return "Yes" if bool(self._value) else "No"
        return str(self._value)

    @property
    def validator(self) -> TableCellValidator | None:
        with self.lock:
            if self._is_set(BaseElementState.HAS_CELL_VALIDATOR_FLAG):
                return cast(TableCellValidator, self.get_property(Property.CellValidator))
            else:
                return None

    @validator.setter
    def validator(self, tcv: Optional[TableCellValidator | Callable]) -> None:
        if callable(tcv):
            tcv = LambdaValidator.build(tcv)
        if isinstance(tcv, TableCellValidator):
            self._set_property(Property.CellValidator, tcv)
        elif tcv is not None:
            raise ValueError(
                f"Validator must be a Lambda expression, TableCellValidator or None, not '{type(tcv).__name__}'"
            )
        else:
            self._clear_property(Property.CellValidator)
        self._mutate_state(BaseElementState.HAS_CELL_VALIDATOR_FLAG, tcv is not None)

    @property
    def transformer(self) -> TableCellTransformer | None:
        return cast(TableCellTransformer, self.validator)

    @transformer.setter
    def transformer(self, tcv: Optional[TableCellTransformer | Callable]) -> None:
        if callable(tcv):
            tcv = LambdaTransformer.build(tcv)
        self.validator = tcv

    def _apply_transform(self, value: Any) -> Any:
        if self.validator:
            return self.validator.transform(value)
        for e in [self.column, self.row]:
            if e and e.cell_validator is not None:
                return e.cell_validator.transform(value)
        return value

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
    def _column(self) -> Column:
        return self._col

    @property
    def row(self) -> Row:
        self.vet_element()
        return self._row

    @property
    def _row(self) -> Row:
        return self.table._row_by_cell_offset(self._offset) if self.table else None  # type: ignore[return-value]

    @property
    def datatype(self) -> Type | None:
        if not self.is_value_error:
            return self._datatype
        else:
            return None

    @property
    def _datatype(self) -> Type | None:
        return type(self._value) if self._value is not None else None

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
            differ = self._set_cell_value_internal(value, type_safe_check=True, preprocess=True)

            if differ:
                if self.table and self.table.is_automatic_recalculate_enabled:
                    # TODO: DerivationImpl.recalculateAffected(this)
                    pass
                self._fire_events(EventType.OnNewValue, old_value, self._value)

    @property
    def uuid(self) -> uuid.UUID:
        with self.lock:
            value = self.get_property(Property.UUID)
            if value is None:
                value = uuid.uuid4()
                self._initialize_property(Property.UUID, value)
            return cast(uuid.UUID, value)

    @uuid.setter
    def uuid(self, value: uuid.UUID | str) -> None:
        with self.lock:
            if self.get_property(Property.UUID):
                pass
            else:
                if isinstance(value, str):
                    self._initialize_property(Property.UUID, uuid.UUID(value))
                elif isinstance(value, uuid.UUID):
                    self._initialize_property(Property.UUID, value)

    def lookup_remote_uuid(self) -> uuid.UUID:  # type: ignore[name-defined]
        for e in [self, self.row, self.column]:
            if e and e.is_derived:  # type: ignore[attr-defined]
                return e.derivation.lookup_remote_uuid_by_cell(self)  # type: ignore[attr-defined]
        return None

    def __increment_pendings(self) -> None:
        if not self.is_pendings:
            self.__set_pending(True)
            for e in [self.table, self.column, self.row]:
                if e:
                    e._increment_pendings()

    def __decrement_pendings(self) -> None:
        if self.is_pendings:
            self.__set_pending(False)
            for e in [self.table, self.column, self.row]:
                if e:
                    e._decrement_pendings()

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
            if e and e.is_datatype_enforced:  # type: ignore[attr-defined]
                return True
        return self.is_enforce_datatype

    @property
    def num_cells(self) -> int:
        return 1

    def fill(self, o: Any, preprocess: Optional[bool] = True) -> None:
        self._set_cell_value_internal(o, type_safe_check=True, preprocess=bool(preprocess))

    def clear(self) -> None:
        return self.fill(None)

    def delete(self) -> None:
        self._set_cell_value_internal(None, type_safe_check=False, preprocess=True)

    @property
    def num_groups(self) -> int:
        return len(self.table._get_cell_groups(self)) if self.table else 0

    @property
    def groups(self) -> Collection[Group]:
        return tuple(self.table._get_cell_groups(self)) if self.table else ()

    def _add_to_group(self, g: Group) -> None:
        if self.table:
            self.table._register_group_cell(self, g)

    def _remove_from_group(self, g: Group) -> None:
        if self.table:
            self.table._deregister_group_cell(self, g)

    def _remove_from_all_groups(self) -> None:
        for g in self.groups:
            g.remove(self)

    @property
    def is_derived(self) -> bool:
        return self._is_set(BaseElementState.IS_DERIVED_CELL_FLAG)

    def _post_result(self, t: Token) -> bool:
        return False

    @property
    def affects(self) -> Collection[Derivable]:
        return self.table._get_cell_affects(self) if self.table else []

    @property
    def derived_elements(self) -> Collection[Derivable]:
        self.vet_element()
        return tuple([self]) if self.is_derived else tuple()

    @property
    def derivation(self) -> Derivation | None:
        return self.table._get_cell_derivation(self) if self.table else None

    def clear_derivation(self) -> None:
        pass

    @property
    def lock(self) -> RLock:
        # TODO: Move out of cell class
        return self._lock
