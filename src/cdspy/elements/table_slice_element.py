from __future__ import annotations

from abc import ABC, abstractmethod
from typing import cast, Optional, Set, TypeVar, Generic
from uuid import UUID

from ..utils import ArrayList
from ..utils import JustInTimeSet

from . import BaseElementState
from . import ElementType
from . import TableElement
from . import TableCellsElement
from . import Group

from ..mixins import Derivable

from ..exceptions import InvalidException
from ..exceptions import UnsupportedException

T = TypeVar("T", bound="TableSliceElement")


class TableSliceElement(TableCellsElement, Derivable, ABC, Generic[T]):
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

    def __init__(self, te: Optional[TableElement] = None) -> None:
        super().__init__(te)
        self._set_index(-1)
        self._set_is_in_use(False)
        self.__remote_uuids: Set[UUID] = set()
        self._groups = JustInTimeSet[Group]()

    def __del__(self) -> None:
        super().__del__()

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
    def is_datatype_enforced(self) -> bool:
        """
        Tables, rows, columns, and cells each can specify if data typing is
        enforced via the is_enforce_datatype property. This property helps to
        walk the object hierarchy backwards from most-general (table) to least-
        general TableElement

        :return: True if data typing is enforced for this object
        """
        if self.is_enforce_datatype:
            return True
        return self.table.is_datatype_enforced if self.table else False

    @property
    def is_nulls_supported(self) -> bool:
        if self.is_supports_null:
            return True
        return self.table.is_nulls_supported if self.table else False

    @property
    def num_groups(self) -> int:
        return len(self._groups)

    def _fill(self, value: object, preserve_current: bool, preserve_derived_cells: bool, fire_events: bool) -> None:
        pass

    def fill(self, value: object) -> None:
        self._fill(value, preserve_current=True, preserve_derived_cells=False, fire_events=True)

    def clear(self) -> None:
        self.fill(None)

    def clear_derivation(self) -> None:
        pass

    def clear_time_series(self) -> None:
        pass
