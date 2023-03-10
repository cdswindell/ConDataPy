from __future__ import annotations

from abc import ABC
from collections.abc import Collection
from threading import Lock
from typing import cast, Final, Optional, TYPE_CHECKING
import uuid

from ordered_set import OrderedSet

from ..exceptions import InvalidParentException

from . import Tag
from . import Property
from . import TableElement

if TYPE_CHECKING:
    from . import Table
    from . import TableContext
    from ..mixins import Derivable


class AtomicInteger:
    def __init__(self, value: int = 0) -> None:
        self._value = int(value)
        self._lock = Lock()

    def inc(self, d: Optional[int] = 1) -> int:
        d = d if d is not None else 1
        with self._lock:
            retval = self._value
            self._value += int(d)
            return retval

    def dec(self, d: Optional[int] = 1) -> int:
        d = d if d is not None else 1
        with self._lock:
            retval = self._value
            self.inc(-d)
            return retval

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    # setter is not used in cdspy, but is included for completeness here
    # @value.setter
    # def value(self, v):
    #     with self._lock:
    #         self._value = int(v)
    #         return self._value


class TableCellsElement(TableElement, ABC):
    ELEMENT_IDENT_GENERATOR: Final = AtomicInteger(1000)

    def __init__(self, te: Optional[TableElement] = None) -> None:
        super().__init__(te)
        self._pendings = 0
        self._table = te.table if te else None
        self._affects = OrderedSet()
        self._initialize_property(Property.Ident, TableCellsElement.ELEMENT_IDENT_GENERATOR.inc())

    def _vet_parent(self, *elems: TableCellsElement) -> None:
        if elems:
            for e in elems:
                if e == self or not e:
                    continue
                with e.lock:
                    # make sure element is valid
                    self._vet_element()
                    # make sure elem belongs to table
                    if not e.table:
                        # if not set, set now
                        e._table = self.table
                    elif e.table != self.table:
                        raise InvalidParentException(e, self)

    def __lt__(self, other: TableCellsElement) -> bool:
        if not isinstance(other, TableCellsElement):
            raise NotImplementedError
        s = self.label if self.label else self.uuid
        o = other.label if other.label else other.uuid
        return s < o

    def _delete(self, compress: Optional[bool] = True) -> None:
        # TODO: Fire events

        # clear label; this resets dependent indices
        self.label = None

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

    @property
    def table(self) -> TableElement:
        return cast(Table, self._table)

    def _set_table(self, table: Table) -> None:
        self._table = table

    @property
    def table_context(self) -> TableContext | None:
        return self.table.table_context if self.table else None

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
    def uuid(self) -> str:
        with self.lock:
            value = self.get_property(Property.UUID)
            if value is None:
                value = uuid.uuid4()
                self._initialize_property(Property.UUID, value)
            return str(value)

    @uuid.setter
    def uuid(self, value: str) -> None:
        with self.lock:
            if not self.get_property(Property.UUID):
                self._initialize_property(Property.UUID, uuid.UUID(value))
