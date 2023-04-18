from __future__ import annotations

from abc import ABC
from collections.abc import Collection
from threading import RLock
from typing import cast, Final, List, Optional, TYPE_CHECKING, Any
import uuid
from weakref import ref

from ordered_set import OrderedSet

from ..exceptions import InvalidParentException

from . import Property, EventType
from . import TableElement

from ..utils.atomic_integer import AtomicInteger


if TYPE_CHECKING:
    from ..mixins import Derivable
    from . import BaseElement
    from . import Table
    from . import TableContext


class TableCellsElement(TableElement, ABC):
    __slots__: List[str] = ["_lock", "_pendings", "_table_ref", "_affects"]

    _ELEMENT_IDENT_GENERATOR: Final = AtomicInteger(1000)

    def __init__(self, te: Optional[TableElement] = None) -> None:
        super().__init__(te)
        self._lock = RLock()
        self._pendings = 0
        self._table_ref = ref(te.table) if te else None
        self._affects = OrderedSet()
        self._initialize_property(Property.Ident, TableCellsElement._ELEMENT_IDENT_GENERATOR.inc())

    def __del__(self) -> None:
        if self.is_valid:
            self._delete()

    def __lt__(self, other: TableCellsElement) -> bool:
        if not isinstance(other, TableCellsElement):
            raise NotImplementedError
        s: str = self.label if self.label else str(self.uuid)
        o: str = other.label if other.label else str(other.uuid)
        return s < o

    def _delete(self, compress: bool = True) -> None:
        # TODO: Fire events

        # clear label; this resets dependent indices
        self.label = None

    def _invalidate(self) -> None:
        super()._invalidate()
        self._table_ref = None

    def _get_template(self, te: Optional[TableElement] = None) -> BaseElement:
        from . import TableContext

        if te:
            return te
        if self.table and self.table != self:
            return self.table
        return self.table_context if self.table_context else TableContext.fetch_default_context()

    def _register_affects(self, elem: Derivable) -> None:
        with self.lock:
            self._affects.add(elem)

    def _deregister_affects(self, elem: Derivable) -> None:
        with self.lock:
            self._affects.remove(elem)

    def _increment_pendings(self) -> None:
        with self.lock:
            self._pendings += 1

    def _decrement_pendings(self) -> None:
        with self.lock:
            self._pendings += 1

    def _clear_affects(self) -> None:
        for a in list(self.affects):
            a.clear_derivation()

    @property
    def table(self) -> Table:
        return self._table_ref() if self._table_ref else None  # type: ignore[return-value]

    def _set_table(self, table: Table) -> None:
        self._table_ref = ref(table) if table else None

    @property
    def table_context(self) -> TableContext:
        return self.table.table_context if self.table else None  # type: ignore[return-value]

    @property
    def lock(self) -> RLock:
        return self._lock

    @property
    def properties(self) -> Collection[Property]:
        return sorted(list(self.element_type.properties()))

    @property
    def is_pendings(self) -> bool:
        return self._pendings > 0

    @property
    def affects(self) -> Collection[Derivable]:
        with self.lock:
            return tuple(self._affects)

    @property
    def ident(self) -> int:
        return cast(int, self.get_property(Property.Ident))

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

    def vet_parent(self, *elems: TableCellsElement) -> None:
        if elems:
            for e in elems:
                if e == self or not e:
                    continue
                with e.lock:
                    # make sure element is valid
                    self.vet_element()
                    # make sure elem belongs to table
                    if not e.table:
                        # if not set, set now; parent could be null...
                        e._set_table(self.table)
                    elif e.table != self.table:
                        raise InvalidParentException(e, self)

    def fire_events(self, te: TableElement, et: EventType, *args: Any) -> None:
        pass
