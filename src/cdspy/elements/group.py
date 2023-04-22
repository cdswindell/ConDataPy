from __future__ import annotations

from collections.abc import Collection, Iterator
from typing import cast, Optional, TYPE_CHECKING, Any, Final
from _weakref import ref

from pyroaring import BitMap
from ordered_set import OrderedSet

from ..mixins import Derivable, Groupable
from ..utils import JustInTimeSet

from . import ElementType, EventType, BaseElementState
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


SHIFT_BY: Final = 16
SHIFT_MASK: Final = (1 << SHIFT_BY) - 1


class _GroupCellIterator:
    def __init__(self, group: Group) -> None:
        self._group = group
        self._table_ref = ref(group.table) if group else None
        self.__iter__()  # initialize iterator fields

    def __iter__(self) -> Iterator[Cell]:
        self._iter = iter(self._group._index_bitmap)
        return self

    def __next__(self) -> Cell:
        encoded_index = next(self._iter)
        r_idx = encoded_index >> SHIFT_BY
        c_idx = encoded_index & SHIFT_MASK
        row = self.table.get_row(r_idx)
        col = self.table.get_column(c_idx)
        return self.table.get_cell(row, col)

    @property
    def table(self) -> Table:
        return self._table_ref() if self._table_ref else None  # type: ignore[return-value]

    @property
    def group(self) -> Group:
        return self._group


class Group(TableCellsElement, Groupable):
    @classmethod
    def _create_group_from_bitmap(cls, t: Table, b: BitMap) -> Group:
        g = cls(t)
        for cell in Group._get_referenced_cells(t, b):
            if cell and cell.is_valid:
                g._add(False, cell)
        return g

    # noinspection PyTypeChecker
    @staticmethod
    def _get_referenced_cells(t: Table, b: BitMap) -> Collection[Cell]:
        cells: set[Cell] = OrderedSet()
        for ei in b:
            ri = ei >> SHIFT_BY
            ci = ei & SHIFT_MASK
            r = t.get_row(ri)
            if r is None or r.is_invalid:
                continue
            c = t.get_column(ci)
            if c is None or c.is_invalid:
                continue
            cell = t.get_cell(r, c)
            if cell and cell.is_valid:
                cells.add(cell)
        return cells

    def __init__(self, parent: Table, label: Optional[str] = None, *elems: TableElement) -> None:
        from . import Table
        from . import Row
        from . import Column
        from . import Cell

        if not isinstance(parent, Table):
            raise TypeError("Table required as first argument")

        super().__init__(parent)
        self.label = label
        self.__cells = JustInTimeSet[Cell]()
        self.__rows = JustInTimeSet[Row]()
        self.__cols = JustInTimeSet[Column]()
        self.__groups = JustInTimeSet[Group]()
        self.__child_groups = JustInTimeSet[Group]()
        self.__num_cells = 0
        self._mark_dirty()

        self.__index_bitmap = BitMap()

        # and mark instance as initialized
        self._mark_initialized()

        # associate to table
        self.table._register_group(self)

        # handle persistence
        if self.table and self.table.is_groups_persistent_default:
            self.is_persistent = True

        # add any elements specified
        if elems:
            self.add(*elems)

    def __contains__(self, x: TableElement | None) -> bool:
        from . import Row
        from . import Column
        from . import Cell

        with self._lock:
            if isinstance(x, TableElement):
                if isinstance(x, Cell):
                    present = x in self.__cells
                    return present or self._contains_cell_reference(x)
                if isinstance(x, Group):
                    return x in self.__groups
                if isinstance(x, Row):
                    return x in self.__rows
                if isinstance(x, Column):
                    return x in self.__cols
            return False

    def __len__(self) -> int:
        return self.num_cells

    def __del__(self) -> None:
        super().__del__()

    def __and__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group &: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        with self.lock:
            nbm = self._index_bitmap & o._index_bitmap
            return Group._create_group_from_bitmap(self.table, nbm)

    def __iand__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group &=: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        with self.lock:
            nbm = self._index_bitmap & o._index_bitmap
            # remove all group elements; they will be replaced with cell references
            self.__purge_components()
            for cell in Group._get_referenced_cells(self.table, nbm):
                if cell and cell.is_valid:
                    self._add(False, cell)
        return self

    def __or__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group |: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        # copy self
        ng = self.copy()
        # Add other group to original
        ng.add(o)
        return ng

    def __ior__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group |: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        # Add other group to original
        self.add(o)
        return self

    def __sub__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group -: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        # create the new, returned group
        ng = Group(self.table)
        # calculate the elements we need to add
        nbm = self._index_bitmap.difference(o._index_bitmap)
        for cell in Group._get_referenced_cells(self.table, nbm):
            if cell and cell.is_valid:
                ng._add(False, cell)
        return ng

    def __isub__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group -: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        # calculate the elements we need to add back
        nbm = self._index_bitmap.difference(o._index_bitmap)
        # clear out existing items
        self.__purge_components()
        for cell in Group._get_referenced_cells(self.table, nbm):
            if cell and cell.is_valid:
                self._add(False, cell)
        return self

    def __xor__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group -: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        # create the new, returned group
        ng = Group(self.table)
        # calculate the elements we need to add
        nbm = self._index_bitmap.symmetric_difference(o._index_bitmap)
        for cell in Group._get_referenced_cells(self.table, nbm):
            if cell and cell.is_valid:
                ng._add(False, cell)
        return ng

    def __ixor__(self, o: Group) -> Group:
        if not isinstance(o, Group):
            raise TypeError(f"unsupported operand type for Group -: '{type(o)}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        # calculate the elements we need to add back
        nbm = self._index_bitmap.symmetric_difference(o._index_bitmap)
        # clear out existing items
        self.__purge_components()
        for cell in Group._get_referenced_cells(self.table, nbm):
            if cell and cell.is_valid:
                self._add(False, cell)
        return self

    def _delete(self, compress: bool = True) -> None:
        if self.is_invalid:
            return
        try:
            super()._delete(compress)
        except BlockedRequestException:
            return

        if self.table:
            self.table._deregister_group(self)

        self.__purge_components()

        self._reset_element_properties()
        self._invalidate()
        self.fire_events(self, EventType.OnDelete)

    def _contains_cell_reference(self, cell: Cell) -> bool:
        cell_ref = (cell.row.index << SHIFT_BY) + cell.column.index
        return cell_ref in self._index_bitmap

    def __purge_components(self) -> None:
        for r in self._rows:
            if r:
                r._remove_from_group(self)
        for c in self._columns:
            if c:
                c._remove_from_group(self)
        for g in self._groups:
            if g:
                g._remove_from_group(self)
        for cl in self._cells:
            if cl:
                cl._remove_from_group(self)
        self.__rows.clear()
        self.__cols.clear()
        self.__groups.clear()
        self.__cells.clear()
        self.__child_groups.clear()
        self.__index_bitmap.clear()
        self.__num_cells = 0
        self._mark_dirty()

    @property
    def element_type(self) -> ElementType:
        return ElementType.Group

    def equal(self, o: object) -> bool:
        if not isinstance(o, Group):
            return False
        if self.table != o.table:
            return False
        return bool(self._index_bitmap == o._index_bitmap)

    def union(self, g: Group) -> Group:
        return self.__or__(g)

    def intersection(self, g: Group) -> Group:
        return self.__and__(g)

    def difference(self, g: Group) -> Group:
        return self.__sub__(g)

    def symmetric_difference(self, g: Group) -> Group:
        return self.__xor__(g)

    def jaccard_index(self, g: Group) -> float:
        if not isinstance(g, Group):
            raise TypeError(f"unsupported operand type for Group.jaccard_index: '{type(g)}'")
        if g.table != self.table:
            return 0.0
        return self._index_bitmap.jaccard_index(g._index_bitmap)

    similarity = jaccard_index

    def is_subset(self, o: Group) -> bool:
        if not isinstance(o, Group):
            raise TypeError(f"Argument 'o' has incorrect type; expected 'Group', got '{type(o).__name__}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        return bool(self._index_bitmap.issubset(o._index_bitmap))

    def is_superset(self, o: Group) -> bool:
        if not isinstance(o, Group):
            raise TypeError(f"Argument 'o' has incorrect type; expected 'Group', got '{type(o).__name__}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        return bool(self._index_bitmap.issuperset(o._index_bitmap))

    def is_disjoint(self, o: Group) -> bool:
        if not isinstance(o, Group):
            raise TypeError(f"Argument 'o' has incorrect type; expected 'Group', got '{type(o).__name__}'")
        if o.table != self.table:
            raise InvalidParentException(self, o)
        return bool(self._index_bitmap.isdisjoint(o._index_bitmap))

    def copy(self) -> Group:
        with self.lock:
            ng = Group(self.table)
            for r in self.rows:
                if r and r.is_valid:
                    ng._add(False, r)
            for c in self.columns:
                if c and c.is_valid:
                    ng._add(False, c)
            for cl in self.__cells:
                if cl and cl.is_valid:
                    ng._add(False, cl)
            for g in self.groups:
                if g and g.is_valid:
                    ng._add(False, g)
            return ng

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table.is_group_labels_indexed) if self.table else False

    def _recalculate_index_bitmap(self, force_it: Optional[bool] = False) -> BitMap:
        with self.lock:
            if self.is_dirty or bool(force_it):
                self.__index_bitmap.clear()

                rows = self._effective_rows
                row_index = 1

                cols = self._effective_columns
                col_index = 1

                for r in rows:
                    r_idx = r.index if r else row_index
                    row_index += 1
                    for c in cols:
                        c_idx = c.index if c else col_index
                        # encode the cell index in the bitmap
                        self.__index_bitmap.add((r_idx << SHIFT_BY) + c_idx)
                        col_index += 1
                    col_index = 1

                # add cells
                for cell in self.__cells:
                    if cell and cell.is_valid:
                        self.__index_bitmap.add((cell.row.index << SHIFT_BY) + cell.column.index)

                # add group cells
                for g in self.__groups:
                    if g and g.is_valid:
                        self.__index_bitmap |= g._index_bitmap
                # update cell count
                self.__num_cells = len(self.__index_bitmap)
                self._mark_clean()
            return self.__index_bitmap

    @property
    def _index_bitmap(self) -> BitMap:
        with self.lock:
            self._recalculate_index_bitmap()
            return self.__index_bitmap.copy()

    @property
    def num_cells(self) -> int:
        # TODO: need RoaringBitmap implementation to handle 64bit ints
        with self.lock:
            if self.is_dirty:
                self._recalculate_index_bitmap()
            return self.__num_cells

    @property
    def _num_cells(self) -> int:
        return len(self.__cells)

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
    def _num_effective_rows(self) -> int:
        return len(self._effective_rows)

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
    def _num_effective_columns(self) -> int:
        return len(self._effective_columns)

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
        return list(self.__groups)

    @property
    def groups(self) -> Collection[Group]:
        return tuple(self._groups)

    @property
    def _cells(self) -> Collection[Cell]:
        return list(self.__cells)

    @property
    def cells(self) -> Iterator[Cell]:
        return _GroupCellIterator(self)

    def _add_to_group(self, g: Group) -> None:
        self.__child_groups.add(g)

    def _remove_from_group(self, g: Group) -> None:
        self.__child_groups.discard(g)

    def add(self, *elems: TableElement) -> bool:
        return self._add(True, *elems)

    def _add(self, do_mark_dirty: Optional[bool] = True, *elems: TableElement) -> bool:
        from . import Row
        from . import Column
        from . import Group
        from . import Cell

        added_any = False
        if elems:
            try:
                with self.lock:
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
                                    if elem == self:
                                        raise RecursionError("Cannot add group to itself")
                                    added_any = True if elem not in self.__groups else added_any
                                    self.__groups.add(elem)
                                # TODO: Add Row and Column and Back Pointer
                            elif isinstance(elem, Cell):
                                if bool(do_mark_dirty) and added_any:
                                    self._mark_dirty()
                                if not (elem in self.__cells or self._contains_cell_reference(elem)):
                                    added_any = True
                                    self.__cells.add(elem)
                            # set up the back-pointer from the element to this group
                            cast(Groupable, elem)._add_to_group(self)
            finally:
                if added_any:
                    self._mark_dirty()
        return added_any

    def remove(self, *elems: TableSliceElement | Group | Cell) -> None:
        from . import Row
        from . import Column
        from . import Group
        from . import Cell

        if not elems:
            return

        with self.lock:
            for elem in elems:
                cast(Groupable, elem)._remove_from_group(self)
                if isinstance(elem, Row):
                    self.__rows.discard(elem)
                elif isinstance(elem, Column):
                    self.__cols.discard(elem)
                elif isinstance(elem, Group):
                    self.__groups.discard(elem)
                elif isinstance(elem, Cell):
                    self.__cells.discard(elem)
            self._mark_dirty()

    def update(self, elems: Collection[TableElement]) -> bool:
        if elems:
            return self.add(*elems)
        return False

    def fill(self, o: Any, preprocess: Optional[bool] = True) -> None:
        self.vet_element()
        if self.table is None:
            raise InvalidParentException(cast(Table, None), self)
        self.__clear_component_derivations()
        cr = self.table._current_cell
        self.table.disable_automatic_recalculation()
        any_changed = False
        try:
            for cell in self.cells:
                if self.__is_derived_cell(cell):
                    continue
                if cell._set_cell_value_internal(o, preprocess=bool(preprocess)):
                    any_changed = True
            if any_changed:
                self.fire_events(self, EventType.OnNewValue, o)
        finally:
            self.table.enable_automatic_recalculation()
            cr.set_current_cell_reference(self.table)
        if self.table.is_automatic_recalculate_enabled:
            # TODO: recalculate
            pass

    def clear(self) -> None:
        self.fill(None)

    @TableElement.is_persistent.setter  # type: ignore[attr-defined]
    def is_persistent(self, state: bool) -> None:
        if self.table is None:
            raise InvalidParentException(cast(Table, None), self)
        if bool(state):
            self.table._persistent_groups.add(self)
        else:
            self.table._persistent_groups.discard(self)
        self._mutate_state(BaseElementState.IS_PERSISTENT_FLAG, state)

    # if the group consists of only rows or only columns,
    # clear derivations from those elements
    def __clear_component_derivations(self) -> None:
        num_rows = len(self.__rows)
        num_cols = len(self.__cols)

        if num_rows > 0 and num_cols == 0:
            for row in self.__rows:
                if row:
                    row.clear_derivation()
                    row.clear_time_series()
        elif num_rows == 0 and num_cols > 0:
            for col in self.__cols:
                if col:
                    col.clear_derivation()
                    col.clear_time_series()

    @staticmethod
    def __is_derived_cell(cell: Cell) -> bool:
        if cell.is_derived:
            return True
        if cell._row and cell._row.is_derived:
            return True
        if cell._column and cell._column.is_derived:
            return True
        return False

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
