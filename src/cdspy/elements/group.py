from __future__ import annotations

from collections.abc import Collection
from typing import cast, Optional, TYPE_CHECKING

from ordered_set import OrderedSet

from ..mixins import Derivable
from ..utils import JustInTimeSet

from . import ElementType
from . import Property
from . import BaseElement
from . import TableElement
from . import TableCellsElement

from ..exceptions import InvalidParentException
from ..events import BlockedRequestException

if TYPE_CHECKING:
    from . import TableSliceElement
    from . import Table
    from . import Row
    from . import Column
    from . import Cell


class Group(TableCellsElement):
    def __init__(self, parent: Table, label: Optional[str] = None) -> None:
        super().__init__(parent)
        self.label = label
        self.__cells = JustInTimeSet[Cell]()
        self.__rows = JustInTimeSet[Row]()
        self.__cols = JustInTimeSet[Column]()
        self.__groups = JustInTimeSet[Group]()
        self.__num_cells = -1  # flag the need to recalculate

        # and mark instance as initialized
        self._mark_initialized()

    def __contains__(self, x: TableElement | None) -> bool:
        if x and isinstance(x, TableElement):
            with self._lock:
                if isinstance(x, Cell):
                    return x in self.__cells
                if isinstance(x, Group):
                    return x in self.__groups
        return False

    def __del__(self) -> None:
        super().__del__()

    def _delete(self, compress: bool = True) -> None:
        if self.is_invalid:
            return
        try:
            super()._delete(compress)
        except BlockedRequestException:
            return
        # TODO: delete elements
        self._invalidate()

    @property
    def element_type(self) -> ElementType:
        return ElementType.Group

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table.is_group_labels_indexed) if self.table else False

    @BaseElement.label.setter  # type: ignore
    def label(self, value: Optional[str] = None) -> None:
        self._set_property(Property.Label, value)
        if self.label and self.table:
            self.table.is_persistent = True

    @property
    def num_cells(self) -> int:
        with self.lock:
            if self.__num_cells == -1:
                num_cells = len(self._effective_rows) * len(self._effective_columns)
                for g in self.__groups:
                    num_cells += g.num_cells
                num_cells += len(self.__cells)
                self.__num_cells = num_cells
            return self.__num_cells

    @property
    def is_null(self) -> bool:
        return self.num_cells == 0

    @property
    def num_rows(self) -> int:
        return len(self.__rows)

    @property
    def _effective_rows(self) -> Collection[Row]:
        if self.num_rows:
            return self.__rows
        if self.table and self.num_columns:
            return self.table._rows
        return list()

    @property
    def _rows(self) -> Collection[Row]:
        if self.__rows:
            return list(self.__rows)
        else:
            return list()

    @property
    def rows(self) -> Collection[Row]:
        return tuple(sorted(self._rows))

    @property
    def num_columns(self) -> int:
        return len(self.__cols)

    @property
    def _effective_columns(self) -> Collection[Column]:
        if self.num_columns:
            return self.__cols
        if self.table and self.num_rows:
            return self.table._columns
        return list()

    @property
    def _columns(self) -> Collection[Column]:
        if self.__cols:
            return list(self.__cols)
        else:
            return list()

    @property
    def columns(self) -> Collection[Column]:
        return tuple(sorted(self._columns))

    @property
    def num_groups(self) -> int:
        return len(self.__groups)

    @property
    def _groups(self) -> Collection[Group]:
        return self.__groups

    @property
    def groups(self) -> Collection[Group]:
        return tuple(self._groups)

    def add(self, *elems: TableElement) -> bool:
        from . import Row
        from . import Column
        from . import Group
        from . import Cell

        added_any = False
        if elems:
            for elem in elems:
                if elem:
                    elem.vet_element()
                    if elem.table != self.table:
                        raise InvalidParentException(self, elem)
                    if isinstance(elem, TableCellsElement):  # row, column, or group
                        if isinstance(elem, Row):
                            added_any = True if elem not in self.__rows else added_any
                            self.__rows.add(elem)
                        elif isinstance(elem, Column):
                            added_any = True if elem not in self.__cols else added_any
                            self.__cols.add(elem)
                        elif isinstance(elem, Group):
                            added_any = True if elem not in self.__groups else added_any
                            self.__groups.add(elem)
                        # TODO: Add Row and Column and Back Pointer
                    elif isinstance(elem, Cell):
                        added_any = True if elem not in self.__cells else added_any
                        self.__cells.add(elem)
                        # TODO: add back pointer
        if added_any:
            self.__num_cells = -1
        return added_any

    def update(self, elems: Collection[TableElement]) -> bool:
        if elems:
            return self.add(*elems)
        return False

    def fill(self, o: Optional[object]) -> None:
        pass

    def clear(self) -> None:
        self.fill(None)

    def remove(self, te: TableSliceElement | Cell) -> None:
        from . import Row
        from . import Column
        from . import Group
        from . import Cell

        with self.lock:
            if isinstance(te, Row):
                self.__rows.discard(te)
            elif isinstance(te, Column):
                self.__cols.discard(te)
            elif isinstance(te, Group):
                self.__groups.discard(te)
            elif isinstance(te, Cell):
                self.__cells.discard(te)

    @property
    def derived_elements(self) -> Collection[Derivable]:
        self.vet_element()
        derived = OrderedSet()

        for row in self._effective_rows:
            if row and row.is_valid and row.is_derived:
                derived.add(row)
        for col in self._effective_columns:
            if col and col.is_valid and col.is_derived:
                derived.add(col)
        for group in self.__groups:
            if group and group.is_valid:
                derived.update(cast(OrderedSet, group.derived_elements))
        for cell in self.__cells:
            if cell and cell.is_valid and cell.is_derived:
                derived.add(cell)
        return cast(Collection[Derivable], derived)
