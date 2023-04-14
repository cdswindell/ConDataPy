from __future__ import annotations

from abc import ABC, abstractmethod
from typing import cast, List, Optional, Set, TypeVar, Generic, Any, Iterable, TYPE_CHECKING, Callable
from uuid import UUID

from .table import _CellReference
from ..computation import Derivation
from ..utils import ArrayList
from ..utils import JustInTimeSet

from . import BaseElementState, EventType
from . import ElementType
from . import Property
from . import TableElement
from . import TableCellsElement
from . import Group

from ..mixins import Derivable

from ..templates import TableCellValidator, TableCellTransformer, LambdaTransformer, LambdaValidator

from ..exceptions import InvalidException, ReadOnlyException
from ..exceptions import UnsupportedException

if TYPE_CHECKING:
    from . import Cell

T = TypeVar("T", bound="TableSliceElement")


class TableSliceElement(TableCellsElement, Derivable, ABC, Generic[T]):
    __slots__: List[str] = ["_index", "__remote_uuids", "_groups"]

    @abstractmethod
    def mark_current(self) -> T | None:
        pass

    @property
    @abstractmethod
    def num_slices(self) -> int:
        pass

    @property
    @abstractmethod
    def slices_type(self) -> ElementType:
        pass

    @property
    @abstractmethod
    def cells(self) -> Iterable[Cell]:
        pass

    @staticmethod
    def _reindex_slice(elems: ArrayList[T]) -> None:
        index = 1
        for elem in elems:
            if elem is not None:
                elem._set_index(index)
            index += 1

    def __init__(self, te: Optional[TableElement] = None) -> None:
        super().__init__(te)
        self._set_index(-1)
        self._set_is_in_use(False)
        self.__remote_uuids: Set[UUID] = set()
        self._groups = JustInTimeSet[Group]()

    def __repr__(self) -> str:
        if self.is_invalid:
            return super().__repr__()
        else:
            label = ": " + self.label if self.label else f" {self.index}"
            return f"[{self.element_type.name}{label}]"

    def __lt__(self, other: TableCellsElement) -> bool:
        if isinstance(other, TableSliceElement):
            return self.index < other.index
        else:
            return super().__lt__(other)

    def _remove_from_all_groups(self) -> None:
        while self._groups:
            g = self._groups.pop()
            g.remove(self)

    def _insert_slice(self, elems: ArrayList[T], index: int) -> T:
        self.vet_element(allow_uninitialized=True)
        if index < 0:
            raise InvalidException(self, f"{self.element_type.name} insertion index must be >= 0")
        if self.table is None:
            raise UnsupportedException(self, f"{self.element_type.name} must belong to a Table")

        self._set_index(index + 1)
        # index is the position in the elems array where this new elem will be inserted,
        # if index is beyond the current last col, we need to extend the Cols array
        if index > len(elems):
            elems.ensure_capacity(index + 1)
            elems[index] = cast(T, self)
        else:  # insert the new column into the cols array and reindex those pushed forward
            elems.insert(index, cast(T, self))
            for e in elems[index + 1 :]:
                if e:
                    e._set_index(e.index + 1)

        self._mark_initialized()
        self.mark_current()
        return cast(T, self)

    def _clear_derivations(self) -> None:
        pass

    def _clear_timeseries(self) -> None:
        pass

    @property
    def _current_cell(self) -> _CellReference | None:
        return self.table._current_cell if self.table else None

    @property
    def cell_validator(self) -> TableCellValidator | None:
        with self.lock:
            if self._is_set(BaseElementState.HAS_CELL_VALIDATOR_FLAG):
                return cast(TableCellValidator, self.get_property(Property.CellValidator))
            else:
                return None

    @cell_validator.setter
    def cell_validator(self, tcv: Optional[TableCellValidator | Callable]) -> None:
        if tcv:
            if callable(tcv):
                tcv = LambdaValidator.build(tcv)
            self._set_property(Property.CellValidator, tcv)
        else:
            self._clear_property(Property.CellValidator)
        self._mutate_state(BaseElementState.HAS_CELL_VALIDATOR_FLAG, tcv is not None)

    @property
    def cell_transformer(self) -> TableCellTransformer | None:
        return cast(TableCellTransformer, self.cell_validator)

    @cell_transformer.setter
    def cell_transformer(self, tcv: Optional[TableCellTransformer | Callable]) -> None:
        if callable(tcv):
            tcv = LambdaTransformer.build(tcv)
        self.cell_validator = tcv

    @property
    def index(self) -> int:
        return self._index

    def _set_index(self, index: int) -> None:
        self._index = index

    @property
    def is_in_use(self) -> bool:
        return self._is_set(BaseElementState.IN_USE_FLAG)

    def _set_is_in_use(self, value: bool) -> None:
        self._mutate_state(BaseElementState.IN_USE_FLAG, value)

    @property
    def is_write_protected(self) -> bool:
        twp = self.table.is_write_protected if self.table else False
        return self.is_read_only or twp

    @property
    def is_nulls_supported(self) -> bool:
        tsn = self.table.is_nulls_supported if self.table else False
        return self.is_supports_null and tsn

    @property
    def is_datatype_enforced(self) -> bool:
        """
        Tables, rows, columns, and cells each can specify if data typing is
        enforced via the is_enforce_datatype property. This property helps to
        walk the object hierarchy backwards from most-general (table) to least-
        general TableElement

        :return: True if data typing is enforced for this object
        """
        tde = self.table.is_datatype_enforced if self.table else False
        return self.is_enforce_datatype or tde

    @property
    def _remote_uuids(self) -> Set[UUID]:
        return self.__remote_uuids

    def register_remote_uuid(self, uuid: UUID | str) -> None:
        if uuid:
            self.__remote_uuids.add(self._normalize_uuid(uuid))

    def deregister_remote_uuid(self, uuid: UUID | str) -> None:
        if uuid:
            self.__remote_uuids.discard(self._normalize_uuid(uuid))

    def _clear_remote_uuids(self) -> None:
        self.__remote_uuids.clear()

    @property
    def num_groups(self) -> int:
        return len(self._groups)

    def _fill(
        self, o: object, preserve_current: bool, preserve_derived_cells: bool, fire_events: bool, recalculate: bool
    ) -> bool:
        self.vet_element()
        if self.is_read_only:
            raise ReadOnlyException(self, Property.CellValue)
        if o is None and not self.is_supports_null:
            raise ValueError("Cell value can not be set to None")

        cr = self._current_cell if preserve_current else None
        reactivate_auto_recalc = False
        if self.table:
            self.table.disable_automatic_recalculation()
            reactivate_auto_recalc = True

        any_changed = False
        try:
            with self.table.lock:
                self.clear_derivation()
                self.clear_time_series()
                any_changed = self.__fill_element(o, preserve_derived_cells)
                if any_changed:
                    self._set_is_in_use(True)
                    if bool(fire_events):
                        self.fire_events(self, EventType.OnNewValue, o)
        finally:
            if cr is not None:
                cr.set_current_cell_reference(self.table)
            if reactivate_auto_recalc:
                self.table.enable_automatic_recalculation()

        # TODO: recalculate affected
        if bool(recalculate):
            pass
        return any_changed

    def __fill_element(self, o: Any, preserve_derived_cells: bool) -> bool:
        any_changed = False
        read_only_exception_encountered = False
        null_value_exception_encountered = False

        for cell in self.cells:
            if preserve_derived_cells and cell.is_derived:
                continue
            else:
                cell.clear_derivation()
            try:
                if cell._set_cell_value_internal(o, type_safe_check=True, preprocess=True):
                    any_changed = True
            except ReadOnlyException:
                read_only_exception_encountered = True
            except ValueError:
                null_value_exception_encountered = True
        # if any were set, ignore exceptions, otherwise, throw them
        if not any_changed:
            if read_only_exception_encountered:
                raise ReadOnlyException(self, Property.CellValue)
            if null_value_exception_encountered:
                raise ValueError("Cell value can not be set to None")
        return any_changed

    def fill(self, value: object) -> None:
        self._fill(value, preserve_current=True, preserve_derived_cells=False, fire_events=True, recalculate=True)

    def clear(self) -> None:
        self.fill(None)

    def clear_derivation(self) -> None:
        pass

    def clear_time_series(self) -> None:
        pass

    @property
    def derivation(self) -> Derivation | None:
        return None

    @property
    def is_derived(self) -> bool:
        return False
