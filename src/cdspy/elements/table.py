from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from weakref import WeakValueDictionary, WeakKeyDictionary, ref
import threading

from typing import Any, cast, Dict, List, Optional
from typing import overload, Set, TYPE_CHECKING, Tuple, Collection

import uuid

from ..utils import ArrayList, JustInTimeSet
from ..events import BlockedRequestException
from ..exceptions import InvalidException, InvalidParentException
from ..exceptions import UnsupportedException
from ..exceptions import InvalidAccessException

from . import Access
from . import BaseElementState
from . import BaseElement
from . import ElementType
from . import Property
from . import EventType
from . import TableElement
from . import TableCellsElement
from . import TableContext

from ..computation import Token

from ..computation import recalculate_affected
from ..computation import Derivation

from ..templates import TableEventListener

from ..mixins import Derivable

if TYPE_CHECKING:
    from . import T
    from . import TableSliceElement
    from . import Row
    from . import Column
    from . import Cell
    from . import Group
    from .filters import FilteredTable

# create thread local storage, but only once for the module
_THREAD_LOCAL_TABLE_STORAGE = threading.local()


class _CellReference:
    def __init__(self, cr: _CellReference | None = None) -> None:
        self._row = cr.current_row if cr else None
        self._col = cr.current_column if cr else None

    def set_current_cell_reference(self, table: Table) -> None:
        cr = table._current_cell
        cr.current_row = self.current_row
        cr.current_column = self.current_column

    @property
    def current_row(self) -> Row | None:
        return self._row

    @current_row.setter
    def current_row(self, row: Row) -> None:
        if row:
            row.vet_parent(row)
        self._row = row

    @property
    def current_column(self) -> Column | None:
        return self._col

    @current_column.setter
    def current_column(self, col: Column) -> None:
        if col:
            col.vet_parent(col)
        self._col = col


class _TableCellIterator:
    def __init__(self, t: Table) -> None:
        BaseElement.vet_base_element(t)
        self._r_index = self._c_index = 0
        self._table_ref = ref(t) if t else None
        self._num_rows = self.table.num_rows if self.table else 0
        self._num_columns = self.table.num_columns if self.table else 0

    @property
    def table(self) -> Table:
        return self._table_ref() if self._table_ref else None  # type: ignore[return-value]

    def __iter__(self) -> Iterator[Cell]:
        self._r_index = self._c_index = 0

        self.table._ensure_rows_exist()
        self._rows = self.table.rows if self.table else []
        self._num_rows = len(self._rows)

        self.table._ensure_columns_exist()
        self._columns = self.table.columns if self.table else []
        self._num_columns = len(self._columns)
        return self

    @property
    def has_next(self) -> bool:
        BaseElement.vet_base_element(self.table)
        return self._r_index < self._num_rows and self._c_index < self._num_columns

    def __next__(self) -> Cell:
        if not self.has_next:
            raise StopIteration
        col = self._columns[self._c_index]  # type: ignore[index]
        row = self._rows[self._r_index]  # type: ignore[index]
        cell = col._get_cell(row, True, False)
        # increment pointers for next iteration
        self._r_index += 1
        if self._r_index >= self._num_rows:
            self._r_index = 0
            self._c_index += 1
        # return the next cell
        return cell  # type: ignore[no-any-return]


class Table(TableCellsElement):
    _table_class_lock = threading.RLock()
    _quick_access_map = {
        "index": Access.ByIndex,
        "label": Access.ByLabel,
        "ident": Access.ByIdent,
        "tags": Access.ByTags,
        "uuid": Access.ByUUID,
        "description": Access.ByDescription,
    }

    @classmethod
    def table_class_lock(cls) -> threading.RLock:
        return cls._table_class_lock

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        from .filters import FilteredTable
        from . import Row
        from . import Column
        from . import Group

        super().__init__()

        num_rows = self._parse_args(int, "num_rows", 0, TableContext().row_capacity_incr_default, *args, **kwargs)
        num_cols = self._parse_args(int, "num_cols", 1, TableContext().column_capacity_incr_default, *args, **kwargs)
        parent_context = self._parse_args(TableContext, "parent_context", 2, None, *args, **kwargs)
        template_table = self._parse_args(Table, "template_table", 3, None, *args, **kwargs)

        # define Table with superclass
        self._set_table(self)

        # we need a context for default property initialization purposes
        parent_context = (
            parent_context if parent_context else template_table.table_context if template_table else TableContext()
        )
        self._context: TableContext = parent_context

        # finally, with context set, initialize default properties
        for p in ElementType.Table.initializable_properties():
            source = template_table if template_table else parent_context
            self._initialize_property(p, source.get_property(p))

        # Initialize other instance attributes
        self.__rows = ArrayList[Row](
            initial_capacity=max(num_rows, self.row_capacity_incr), capacity_increment=self.row_capacity_incr
        )
        self.__cols = ArrayList[Column](
            initial_capacity=max(num_cols, self.column_capacity_incr), capacity_increment=self.column_capacity_incr
        )

        self._next_row_index = 0
        self._next_column_index = 0

        self._cell_offset_row_map: Dict[int, Row] = {}
        self._unused_cell_offsets = deque[int]()
        self.__next_cell_offset_index = 0

        self._rows_capacity = self._calculate_rows_capacity(num_rows)
        self._columns_capacity = self._calculate_columns_capacity(num_cols)

        self._row_label_index: Dict[str, Row] = {}
        self._col_label_index: Dict[str, Column] = {}
        self._cell_label_index: Dict[str, Cell] = {}
        self._group_label_index: Dict[str, Group] = cast(Dict[str, Group], WeakValueDictionary())

        self._element_label_indexes: Dict[ElementType, Dict[str, TableElement]] = {
            ElementType.Row: self._row_label_index,  # type: ignore[dict-item]
            ElementType.Column: self._col_label_index,  # type: ignore[dict-item]
            ElementType.Cell: self._cell_label_index,  # type: ignore[dict-item]
            ElementType.Group: self._group_label_index,  # type: ignore[dict-item]
        }

        self._filters = JustInTimeSet[FilteredTable]()

        self._groups = JustInTimeSet[Group]()
        self._persistent_groups: Set[Group] = set()

        self._cell_properties: Dict[Cell, Dict] = {}
        self._cell_groups: Dict[Cell, Set[Group]] = {}
        self._cell_affects: Dict[Cell, Set[Derivable]] = {}
        self._cell_derivations: Dict[Cell, Derivation] = {}

        self._ident_index: WeakValueDictionary[int, TableCellsElement] = WeakValueDictionary()
        self._uuid_index: WeakValueDictionary[uuid.UUID, TableCellsElement] = WeakValueDictionary()

        self.__table_creation_thread = ref(threading.current_thread())

        # register table with context
        parent_context._register(self)

        # handle default persistence
        if parent_context.is_tables_persistent_default:
            self.is_persistent = True

        # and mark instance as initialized
        self._mark_initialized()

    def delete(self, *elems: TableElement) -> None:
        if elems:
            deleted_any = False
            for elem in elems:
                if elem and elem.is_valid and elem.table == self:
                    elem._delete(False)
                    del elem
                    deleted_any = True

            if deleted_any:
                self._reclaim_row_space()
                self._reclaim_column_space()
                # mark all groups as dirty, forcing recalc of composition
                for g in self._groups:
                    if g and g.is_valid:
                        g._mark_dirty()
                recalculate_affected(self)
        else:
            # delete the entire table
            self._delete(True)

    def _delete(self, compress: bool = True) -> None:
        if self.is_invalid:
            return
        try:
            super()._delete(compress)
        except BlockedRequestException:
            return

        with self.lock:
            try:
                # delete filters first
                while self._filters:
                    ft = self._filters.pop()
                    ft.delete()

                # explicitly delete columns and rows
                for index in range(self.num_columns, 0, -1):
                    c = self._columns[index - 1]
                    if c is not None:
                        c._delete()

                for index in range(self.num_rows, 0, -1):
                    r = self._rows[index - 1]
                    if r is not None:
                        r._delete()

                if compress:
                    self._reclaim_column_space()
                    self._reclaim_row_space()

                for x in self._element_label_indexes.values():
                    x.clear()
                self._element_label_indexes.clear()
                self._groups.clear()
                self._persistent_groups.clear()
                self._affects.clear()
                self._cell_offset_row_map.clear()
                self._unused_cell_offsets.clear()
                self._cell_properties.clear()
                self._cell_groups.clear()
                self._cell_affects.clear()
                self._cell_derivations.clear()
                self.__rows.clear()
                self.__cols.clear()
                self._ident_index.clear()
                self._uuid_index.clear()
            finally:
                self._clear_current_cell()
                self._reset_element_properties()
                self._invalidate()
                if self.table_context:
                    self.table_context._deregister(self)
                    self._context = cast(TableContext, None)
                self.fire_events(self, EventType.OnDelete)

    def _get_cell_element_properties(self, cell: Cell, create_if_empty: bool = False) -> Optional[Dict]:
        if cell:
            with cell.lock:
                cell_props = self._cell_properties.get(cell, None)
                if bool(create_if_empty) and cell_props is None:
                    cell_props = dict()
                    self._cell_properties[cell] = cell_props
                return cell_props
        return None

    def _reset_cell_element_properties(self, cell: Cell) -> None:
        if cell:
            self._cell_properties.pop(cell, None)

    def _register_filter(self, ft: FilteredTable) -> None:
        self._filters.add(ft)

    def _deregister_filter(self, ft: FilteredTable) -> None:
        self._filters.discard(ft)

    def _get_cell_listeners(self, cell: Cell) -> List[TableEventListener]:
        return []

    def _has_cell_listeners(self, cell: Cell) -> bool:
        return False

    def _add_cell_listeners(self, cell: Cell, et: EventType, *listeners: TableEventListener) -> bool:
        return False

    def _remove_cell_listeners(self, cell: Cell, et: EventType, *listeners: TableEventListener) -> bool:
        return False

    def _remove_all_cell_listeners(self, cell: Cell, *events: EventType) -> List[TableEventListener]:
        return []

    def _register_group(self, g: Group) -> None:
        with self.lock:
            self.vet_parent(g)
            table_changed = g not in self._groups
            self._groups.add(g)
            if table_changed:
                self._mark_dirty()

    def _deregister_group(self, g: Group) -> None:
        with self.lock:
            self.vet_parent(g)
            if g.ident in self._ident_index:
                del self._ident_index[g.ident]
            self._groups.discard(g)
            self._persistent_groups.discard(g)

    def _set_persistent_group(self, g: Group, state: bool) -> None:
        if bool(state):
            self._persistent_groups.add(g)
        else:
            self._persistent_groups.discard(g)

    def _register_group_cell(self, cell: Cell, group: Group) -> bool:
        groups = self._cell_groups.get(cell, None)
        if groups is None:
            groups = set()
            self._cell_groups[cell] = groups
        preexists = group in groups
        groups.add(group)
        return preexists

    def _deregister_group_cell(self, cell: Cell, group: Group) -> bool:
        groups = self._cell_groups.get(cell, None)
        if groups is None:
            return False
        else:
            existed = group in groups
            groups.discard(group)
            if len(groups) == 0:
                del self._cell_groups[cell]
            return existed

    def _get_cell_groups(self, cell: Cell) -> Set[Group]:
        return self._cell_groups.get(cell, set())

    def _register_cell_affects(self, cell: Cell, d: Derivable) -> None:
        pass

    def _deregister_cell_affects(self, cell: Cell, d: Derivable) -> None:
        pass

    def _get_cell_affects(self, cell: Cell, include_indirects: bool = True) -> List[Derivable]:
        affected = self._cell_affects.get(cell, set())
        num_affects = len(affected)
        affects: Set[Derivable] = set()
        if num_affects > 0:
            affects.update(affected)

        if bool(include_indirects):
            if cell.column:
                affects.update(cell.column.affects)
            if cell.row:
                affects.update(cell.row.affects)

        # remove cell to avoid cycles
        affects.discard(cell)
        return list(affects)

    def _get_cell_derivation(self, cell: Cell) -> Derivation:
        return self._cell_derivations.get(cell, cast(Derivation, None))

    def _register_cell_derivation(self, cell: Cell, d: Derivation) -> Derivation:
        if cell and d:
            with cell.lock:
                cell._set(BaseElementState.IS_DERIVED_CELL_FLAG)
                old_d = self._cell_derivations.get(cell, None)
                self._cell_derivations[cell] = d
                return cast(Derivation, old_d)
        return cast(Derivation, None)

    @property
    def free_space_threshold(self) -> float:
        return cast(float, self.get_property(Property.FreeSpaceThreshold))

    @free_space_threshold.setter
    def free_space_threshold(self, default: float) -> None:
        self._set_property(Property.FreeSpaceThreshold, default)

    @property
    def row_capacity_incr(self) -> int:
        return cast(int, self.get_property(Property.RowCapacityIncr))

    @row_capacity_incr.setter
    def row_capacity_incr(self, default: int) -> None:
        self._set_property(Property.RowCapacityIncr, default)

    @property
    def column_capacity_incr(self) -> int:
        return cast(int, self.get_property(Property.ColumnCapacityIncr))

    @column_capacity_incr.setter
    def column_capacity_incr(self, default: int) -> None:
        self._set_property(Property.ColumnCapacityIncr, default)

    @property
    def is_automatic_recalculate_enabled(self) -> bool:
        return self.is_automatic_recalculation and not self._is_set(BaseElementState.AUTO_RECALCULATE_DISABLED_FLAG)

    @property
    def is_automatic_recalculation(self) -> bool:
        return self._is_set(BaseElementState.AUTO_RECALCULATE_FLAG)

    @is_automatic_recalculation.setter
    def is_automatic_recalculation(self, state: bool) -> None:
        self._mutate_state(BaseElementState.AUTO_RECALCULATE_FLAG, state)

    def enable_automatic_recalculation(self) -> None:
        self._reset(BaseElementState.AUTO_RECALCULATE_DISABLED_FLAG)

    def disable_automatic_recalculation(self) -> None:
        self._set(BaseElementState.AUTO_RECALCULATE_DISABLED_FLAG)

    def _cache_cell_offset(self, offset: int) -> None:
        if offset >= 0:
            with self.lock:
                self._unused_cell_offsets.append(offset)
                for c in self._columns:
                    if c:
                        c._invalidate_cell(offset)

    @property
    def _next_cell_offset(self) -> int:
        with self.lock:
            if self._unused_cell_offsets:
                try:
                    return self._unused_cell_offsets.popleft()
                except IndexError:
                    pass  # this shouldn't happen because of lock
            self.__next_cell_offset_index += 1
            return self.__next_cell_offset_index - 1

    def _map_cell_offset_to_row(self, row: Row) -> None:
        if row and row._cell_offset >= 0:
            self._cell_offset_row_map[row._cell_offset] = row

    def _row_by_cell_offset(self, offset: int) -> Row:
        if int(offset) >= 0:
            with self.lock:
                try:
                    return self._cell_offset_row_map[offset]
                except KeyError:
                    pass
        return cast(Row, None)

    def __index_element_labels(self, elems: Collection[T], label_index: Dict[str, T], flag: BaseElementState) -> None:
        with self.lock:
            label_index.clear()

            if not elems:
                self._reset(flag)
            else:
                for elem in elems:
                    if elem:
                        label = elem.label
                        if label:
                            label = label.strip().lower()
                            if label in label_index:
                                label_index.clear()
                                self._reset(flag)
                                raise KeyError(f"{elem.element_type.name} Label '{elem.label}' not unique")
                            else:
                                label_index[label] = elem

    @property
    def is_column_labels_indexed(self) -> bool:
        return self._is_set(BaseElementState.COLUMN_LABELS_INDEXED_FLAG)

    @is_column_labels_indexed.setter
    def is_column_labels_indexed(self, state: bool) -> None:
        state = bool(state)
        if state:
            self.__index_element_labels(
                self._columns, self._col_label_index, BaseElementState.COLUMN_LABELS_INDEXED_FLAG
            )
        else:
            self._col_label_index.clear()
        self._mutate_state(BaseElementState.COLUMN_LABELS_INDEXED_FLAG, state)

    @property
    def is_row_labels_indexed(self) -> bool:
        return self._is_set(BaseElementState.ROW_LABELS_INDEXED_FLAG)

    @is_row_labels_indexed.setter
    def is_row_labels_indexed(self, state: bool) -> None:
        state = bool(state)
        if state:
            self.__index_element_labels(self._rows, self._row_label_index, BaseElementState.ROW_LABELS_INDEXED_FLAG)
        else:
            self._row_label_index.clear()
        self._mutate_state(BaseElementState.ROW_LABELS_INDEXED_FLAG, state)

    @property
    def is_cell_labels_indexed(self) -> bool:
        return self._is_set(BaseElementState.CELL_LABELS_INDEXED_FLAG)

    @property
    def is_group_labels_indexed(self) -> bool:
        return self._is_set(BaseElementState.GROUP_LABELS_INDEXED_FLAG)

    @is_group_labels_indexed.setter
    def is_group_labels_indexed(self, state: bool) -> None:
        state = bool(state)
        if state:
            self.__index_element_labels(
                self._groups, self._group_label_index, BaseElementState.GROUP_LABELS_INDEXED_FLAG
            )
        else:
            self._group_label_index.clear()
        self._mutate_state(BaseElementState.GROUP_LABELS_INDEXED_FLAG, state)

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table_context.is_table_labels_indexed) if self.table else False

    @property
    def rows_capacity(self) -> int:
        return self._rows_capacity

    @property
    def columns_capacity(self) -> int:
        return self._columns_capacity

    @property
    def _rows(self) -> ArrayList[Row]:
        return self.__rows

    @property
    def _columns(self) -> ArrayList[Column]:
        return self.__cols

    def _calculate_rows_capacity(self, num_required: int) -> int:
        capacity = self.row_capacity_incr
        if num_required > 0:
            remainder = num_required % capacity
            capacity = num_required + (capacity - remainder if remainder > 0 else 0)
        return capacity

    def _calculate_columns_capacity(self, num_required: int) -> int:
        capacity = self.column_capacity_incr
        if num_required > 0:
            remainder = num_required % capacity
            capacity = num_required + (capacity - remainder if remainder > 0 else 0)
        return capacity

    # noinspection DuplicatedCode
    def _reclaim_column_space(self) -> None:
        if len(self.__cols) == 0:
            self._cell_offset_row_map.clear()
            self._unused_cell_offsets.clear()
            if self.__next_cell_offset_index > 0:
                for r in self._rows:
                    r._set_cell_offset(-1)
            self.__next_cell_offset_index = 0

        if self.free_space_threshold > 0:
            free_cols = self._columns.capacity - len(self._columns)
            incr = self.column_capacity_incr
            ratio = float(free_cols) / incr

            if ratio > self.free_space_threshold or len(self._columns) == 0:
                self._columns.trim()
                self._columns.capacity_increment = incr

    def _reclaim_row_space(self) -> None:
        if len(self.__rows) == 0:
            self._cell_offset_row_map.clear()
            self._unused_cell_offsets.clear()
            if self.__next_cell_offset_index > 0:
                for c in self._columns:
                    if c:
                        c._reclaim_cell_space(self._rows, 0)
            self.__next_cell_offset_index = 0

        if self.free_space_threshold > 0:
            free_rows = self._rows.capacity - len(self._rows)
            incr = self.row_capacity_incr
            ratio = float(free_rows) / incr

            if ratio > self.free_space_threshold or len(self._rows) == 0:
                self._rows.trim()
                self._rows.capacity_increment = incr

    def _reclaim_cell_space(self, num_rows: int) -> None:
        num_cols = len(self._columns)
        if num_cols > 0:
            for c in self._columns:
                if c is not None:
                    c._reclaim_cell_space(self._rows, num_rows)
        if self.__next_cell_offset_index > num_rows:
            self._cell_offset_row_map.clear()
            cell_offset = 0
            offset = -1
            if num_rows > 0:
                for r in self._rows:
                    if r is not None and r._cell_offset >= 0:
                        if num_cols > 0:
                            offset = cell_offset
                            cell_offset += 1
                        r._set_cell_offset(offset)
            self._unused_cell_offsets.clear()
            self.__next_cell_offset_index = cell_offset

    def get_cell(self, row: Row, col: Column) -> Cell:
        self.vet_elements(row, col)
        return self._get_cell(row, col, True)  # type: ignore[return-value]

    def is_cell(self, row: Row, col: Column) -> bool:
        return self._get_cell(row, col, False) is not None

    def get_cell_value(self, row: Row, col: Column, do_format: bool = False) -> Any:
        cell = self._get_cell(row, col, False)
        if cell:
            if bool(do_format):
                return cell.formatted_value
            else:
                return cell.value
        else:
            return None

    def get_formatted_cell_value(self, row: Row, col: Column) -> Any:
        cell = self._get_cell(row, col, False)
        if cell:
            return cell.formatted_value
        else:
            return None

    def _get_cell(
        self, row: Row, col: Column, create_if_sparse: bool = True, set_to_current: bool = True
    ) -> Cell | None:
        if (row is None) or (col is None):
            return None  # type: ignore[unreachable]
        self.vet_parent(row, col)
        with self.lock:
            return col._get_cell(row, create_if_sparse=create_if_sparse, set_to_current=set_to_current)

    def set_cell_value(self, row: Row, col: Column, o: Any) -> None:
        self.vet_elements(row, col)
        with self.lock:
            cell = self._get_cell(row, col, o is not None)
            if cell is not None:
                if isinstance(o, Token):
                    cell._post_result(o)
                else:
                    cell.value = o

    def _get_table_cell(self, t: Table, r: Row, c: Column, create_if_sparse: bool, set_current: bool) -> Cell:
        from . import Cell

        t.vet_parent(r, c)
        with self.lock:
            return cast(Cell, c._get_cell(r, create_if_sparse, set_current))

    def _fire_cell_events(self, cell: Cell, evt: EventType, *args: Any) -> None:
        pass

    @property
    def _num_cells(self) -> int:
        # todo: implement
        return 0

    @property
    def table(self) -> Table:
        return self

    @property
    def table_context(self) -> TableContext:
        return self._context

    @property
    def element_type(self) -> ElementType:
        return ElementType.Table

    @BaseElement.label.setter  # type: ignore[attr-defined]
    def label(self, value: Optional[str]) -> None:
        old_label = self.label
        try:
            self._set_property(Property.Label, value)
            if self.table_context:
                self.table_context._index_table_label(self)
        except KeyError as e:
            self._set_property(Property.Label, old_label)
            raise e

    @property
    def is_datatype_enforced(self) -> bool:
        return self.is_enforce_datatype or self.table_context.is_enforce_datatype if self.table_context else False

    @property
    def is_nulls_supported(self) -> bool:
        return self.is_supports_null and self.table_context.is_supports_null if self.table_context else False

    @property
    def num_rows(self) -> int:
        return len(self._rows)

    @property
    def num_columns(self) -> int:
        return len(self._columns)

    @property
    def num_cells(self) -> int:
        self.vet_element()
        num_cells = 0
        for c in self._columns:
            num_cells += c.num_cells if c else 0
        return num_cells

    @property
    def is_null(self) -> bool:
        return self.num_rows == 0 or self.num_columns == 0 or self.num_cells == 0

    @property
    def num_groups(self) -> int:
        return len(self._groups)

    @property
    def are_cell_labels_indexed(self) -> bool:
        return False

    def fill(self, o: Any, preprocess: Optional[bool] = True) -> None:
        with self.lock:
            self.vet_element()

            any_changed = False
            cr = self._current_cell
            self.disable_automatic_recalculation()
            try:
                col = self.get_column(Access.First)
                while col:
                    if col._fill(
                        o,
                        preserve_current=False,
                        preserve_derived_cells=False,
                        preprocess=bool(preprocess),
                        fire_events=False,
                        recalculate=False,
                    ):
                        any_changed = True
                    col = self.get_column(Access.Next)
            finally:
                self.enable_automatic_recalculation()
                cr.set_current_cell_reference(self)
        # there is no need to recalculate table, as all derivations are cleared with this operation
        if any_changed:
            self.fire_events(self, EventType.OnNewValue, o)

    def clear(self) -> None:
        self.fill(None)

    @property
    def cells(self) -> Iterator[Cell]:
        return _TableCellIterator(self)

    @property
    def is_write_protected(self) -> bool:
        return self.is_read_only or self.table_context.is_read_only if self.table_context else False

    @property
    def derived_elements(self) -> Collection[Derivable]:
        return []

    @property
    def is_groups_persistent_default(self) -> bool:
        return cast(bool, self.get_property(Property.IsGroupsPersistentDefault))

    @is_groups_persistent_default.setter
    def is_groups_persistent_default(self, default: bool) -> None:
        self._set_property(Property.IsGroupsPersistentDefault, default)

    @TableElement.is_persistent.setter  # type: ignore[attr-defined]
    def is_persistent(self, state: bool) -> None:
        with self.lock:
            self._mutate_state(BaseElementState.IS_PERSISTENT_FLAG, state)  # type: ignore
            if self.is_initialized and self.table_context:
                self.table_context._register(self)

    @property
    def _current_cell(self) -> _CellReference:
        """
        Maintain a "current cell" independently in each thread that accesses this table
        :return:
        """
        global _THREAD_LOCAL_TABLE_STORAGE
        with self.lock:
            try:
                return cast(_CellReference, _THREAD_LOCAL_TABLE_STORAGE._current_cell_map[self])
            except AttributeError:
                with Table.table_class_lock():
                    _THREAD_LOCAL_TABLE_STORAGE._current_cell_map = WeakKeyDictionary[Table, _CellReference]()
                _THREAD_LOCAL_TABLE_STORAGE._current_cell_map[self] = _CellReference()
                return _THREAD_LOCAL_TABLE_STORAGE._current_cell_map[self]
            except KeyError:
                _THREAD_LOCAL_TABLE_STORAGE._current_cell_map[self] = _CellReference()
                return cast(_CellReference, _THREAD_LOCAL_TABLE_STORAGE._current_cell_map[self])

    def _clear_current_cell(self) -> None:
        global _THREAD_LOCAL_TABLE_STORAGE
        with self.lock:
            try:
                del _THREAD_LOCAL_TABLE_STORAGE._current_cell_map[self]
            except AttributeError:
                pass
            except KeyError:
                pass

    @property
    def current_row(self) -> Row | None:
        return self._current_cell.current_row

    @current_row.setter
    def current_row(self, row: Row | None) -> None:
        self._current_cell.current_row = row

    @property
    def current_column(self) -> Column | None:
        return self._current_cell.current_column

    @current_column.setter
    def current_column(self, col: Column | None) -> None:
        self._current_cell.current_column = col

    @overload
    def mark_current(self, new_current: Row) -> Row | None:
        ...

    @overload
    def mark_current(self, new_current: Column) -> Column | None:
        ...

    @overload
    def mark_current(self, new_current: Cell) -> Cell | None:
        ...

    def mark_current(self, new_current: Row | Column | Cell | None = None) -> Row | Column | Cell | None:
        from . import Row
        from . import Column

        prev: Row | Column | Cell | None = None
        if isinstance(new_current, Column):
            prev = self._current_cell.current_column
            self._current_cell.current_column = new_current
        elif isinstance(new_current, Row):
            prev = self._current_cell.current_row
            self._current_cell.current_row = new_current
        return prev

    @property
    def _current_cell_stack(self) -> deque[_CellReference]:
        global _THREAD_LOCAL_TABLE_STORAGE
        stack_map = None
        with self.lock:
            try:
                return cast(deque[_CellReference], _THREAD_LOCAL_TABLE_STORAGE._current_cell_stack[self])
            except AttributeError:
                with Table.table_class_lock():
                    _THREAD_LOCAL_TABLE_STORAGE._current_cell_stack = WeakKeyDictionary[Table, deque[_CellReference]]()
                _THREAD_LOCAL_TABLE_STORAGE._current_cell_stack[self] = deque[_CellReference]()
                return _THREAD_LOCAL_TABLE_STORAGE._current_cell_stack[self]
            except KeyError:
                _THREAD_LOCAL_TABLE_STORAGE._current_cell_stack[self] = deque[_CellReference]()
                return cast(deque[_CellReference], _THREAD_LOCAL_TABLE_STORAGE._current_cell_stack[self])

    def pop_current_cell(self) -> None:
        cr = self._current_cell_stack.popleft() if self._current_cell_stack else None
        if cr:
            cr.set_current_cell_reference(self)

    def push_current_cell(self) -> None:
        self.vet_element()
        self._current_cell_stack.appendleft(_CellReference(self._current_cell))

    def _purge_current_stack(self, sl: TableSliceElement) -> None:
        pass

    def _calculate_index(self, et: ElementType, is_adding: bool, access: Access, *mda: object) -> int:
        from . import TableSliceElement

        is_adding = bool(is_adding)

        if et == ElementType.Row:
            cur_slice: TableSliceElement | None = self.current_row
            num_slices: int = self.num_rows
            slices: ArrayList[Row] = self._rows
        elif et == ElementType.Column:
            cur_slice: TableSliceElement | None = self.current_column
            num_slices: int = self.num_columns
            slices: ArrayList[Column] = self._columns
        else:
            raise InvalidException(self, f"{et.name} not supported")

        # if we are doing a get (not adding), and num_slices is 0, we're done
        if not is_adding and num_slices == 0:
            return -1

        md = mda[0] if mda else None
        if access == Access.ByIndex:
            if md is None or not isinstance(md, int):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            index = int(md) - 1
            if index < 0:
                return -1
            elif is_adding or index < num_slices:
                return index
            else:
                return -1
        elif access == Access.ByIdent:
            if is_adding or md is None or not isinstance(md, int):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            target = cast(TableSliceElement, self._ident_index.get(int(md), None))
            return int(target.index) - 1 if target and target.element_type == et else -1
        elif access in [Access.ByLabel, Access.ByDescription]:
            if is_adding or md is None or not isinstance(md, str):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            if (
                access == Access.ByLabel
                and (et == ElementType.Row and self.is_row_labels_indexed)
                or (et == ElementType.Column and self.is_column_labels_indexed)
            ):
                key = str(md).strip().lower()
                target = self._element_label_indexes[et].get(key, None) if key else None  # type: ignore
            else:
                target = cast(TableSliceElement, self._find(slices, access.associated_property, str(md)))
            return int(target.index) - 1 if target else -1
        elif access == Access.ByUUID:
            if is_adding or md is None or (not isinstance(md, str) and not isinstance(md, uuid.UUID)):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            md = md if isinstance(md, uuid.UUID) else uuid.UUID(str(md))  # type: ignore[unreachable]
            target = cast(TableSliceElement, self._uuid_index.get(md, None))
            return int(target.index) - 1 if target and target.element_type == et else -1
        elif access == Access.ByTags:
            if is_adding or not mda or any(not isinstance(item, str) for item in mda):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {mda}")
            target = cast(TableSliceElement, self._find_tagged(slices, *cast(Tuple[str], mda)))
            return int(target.index) - 1 if target else -1
        elif access == Access.First:
            return 0
        elif access == Access.Last:
            if is_adding:
                return num_slices
            else:
                return num_slices - 1 if num_slices else -1
        elif access == Access.Previous:
            # special case for adding to an empty table
            if is_adding and num_slices == 0:
                return 0
            # if no current row/col, we can't honor request
            if cur_slice is None:
                return -1
            # if adding, insert slice before current
            # if retrieving, return previous
            index = cur_slice.index - 1
            if is_adding:
                return index
            # if we're at the first slice, there is no previous
            return index - 1 if index > 0 else -1
        elif access == Access.Current:
            # special case for adding to an empty table
            if is_adding and num_slices == 0:
                return 0
            return cur_slice.index - 1 if cur_slice is not None else -1
        elif access == Access.Next:
            # special case for adding to an empty table
            if is_adding and num_slices == 0:
                return 0
            # if no current row/col, we can't honor request
            if cur_slice is None:
                return -1
            index = cur_slice.index
            if index < num_slices:
                return index
            elif is_adding and index == num_slices:
                return index
            else:
                return -1
        elif access == Access.ByReference:
            if is_adding or md is None or not isinstance(md, TableSliceElement) or md.element_type != et:
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            if md.table != self:
                raise InvalidParentException(self, md)
            return int(md.index) - 1
        elif access == Access.ByProperty:
            key: Property | str = md  # type: ignore[assignment]
            value = mda[1] if mda and len(mda) > 1 else None
            if is_adding or key is None or value is None:
                raise InvalidException(self, f"Invalid {et.name} {access.name} key: {key}")
            target = cast(TableSliceElement, self._find(slices, key, value))
            return int(target.index) - 1 if target else -1
        elif access == Access.ByDataType:
            if et != ElementType.Column:
                raise InvalidException(self, f"{access.name} only valid for Columns")
            if is_adding or md is None or (not isinstance(md, str) and not isinstance(md, type)):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {cast(type, md).__name__}")
            is_exact = bool(mda[1]) if mda and len(mda) > 1 else True
            if is_exact:
                target = cast(TableSliceElement, self._find(slices, Property.DataType, md))
            else:
                target = None
            return int(target.index) - 1 if target else -1
        return -1

    def _add_slice_dispatch(
        self, et: ElementType, a1: int | Access | None = None, *args: object
    ) -> Row | Column | None:
        self.vet_element()
        if a1 is None:
            return self._add_slice(et, Access.Last, False, True, True)  # type: ignore[return-value]
        elif isinstance(a1, int):
            return self._add_slice(et, Access.ByIndex, False, True, True, int(a1))  # type: ignore[return-value]
        elif isinstance(a1, Access):
            return self._add_slice(et, a1, self.is_row_labels_indexed, True, True, *args)  # type: ignore[return-value]
        else:
            raise UnsupportedException(self, "Cannot insert Row into Table with these arguments")

    def _add_slice(
        self,
        slice_type: ElementType,
        access: Access,
        return_existing: bool = False,
        create_if_sparse: bool = False,
        set_to_current: bool = False,
        *mda: object,
    ) -> Row | Column | None:
        from . import Row
        from . import Column

        insert_mode = access

        # for modes that may enforce uniqueness, do some checks
        if access in [Access.ByLabel, Access.ByUUID, Access.ByDescription]:
            slices: ArrayList[Row] | ArrayList[Column] = self._rows if slice_type == ElementType.Row else self._columns
            existing_slice = self._get_slice(slice_type, slices, access, create_if_sparse, set_to_current, *mda)
            if existing_slice is not None:
                if bool(return_existing):
                    return existing_slice
                # allow dups? specified in mda[1]
                allow_dups = (
                    bool(mda[1])
                    if access != Access.ByUUID and mda and len(mda) > 1 and isinstance(mda[1], bool)
                    else False
                )
                if not allow_dups:
                    raise InvalidException(self, f"{slice_type.name} with {access.name} '{mda[0]}' exists")
            insert_mode = Access.Last  # add new slice as last
        elif access == Access.ByDataType:
            if slice_type != ElementType.Column:
                raise InvalidException(self, f"{access.name} only valid for Columns")
            existing_slice = self.get_column(access, *mda)
            if existing_slice:
                if bool(return_existing):
                    return existing_slice
                else:
                    allow_dups = bool(mda[1]) if mda and len(mda) > 1 and isinstance(mda[1], bool) else False
                    if not allow_dups:
                        dt = cast(type, mda[0])
                        raise InvalidException(self, f"{slice_type.name} with {access.name} '{dt.__name__}' exists")
            insert_mode = Access.Last  # add new slice as last

        # create a new row/column object and insert it into table
        with self.lock:
            raw_te: Row | Column = Row(self) if slice_type == ElementType.Row else Column(self)
            te = self._insert_slice(raw_te, insert_mode, set_to_current, True, *mda)
            # do post_processing
            if te and mda:
                if access == Access.ByLabel:
                    te.label = str(mda[0])
                elif access == Access.ByUUID:
                    te.uuid = mda[0] if isinstance(mda[0], uuid.UUID) else uuid.UUID(str(mda[0]))
                elif access == Access.ByDescription:
                    te.description = str(mda[0])
                elif access == Access.ByDataType:
                    te.datatype = mda[0]  # type: ignore[union-attr]
            # mark all groups as dirty, forcing recalc of composition
            for g in self._groups:
                if g and g.is_valid:
                    g._mark_dirty()
            return te  # type: ignore[return-value]

    def _insert_slice(
        self, te: Row | Column, access: Access, set_to_current: bool = True, fire_events: bool = True, *mda: object
    ) -> None | Row | Column:
        try:
            if bool(fire_events):
                pass
        except BlockedRequestException:
            return None
        successfully_created = False
        try:
            index = self._calculate_index(te.element_type, True, access, *mda)
            if index <= -1:
                raise InvalidAccessException(self, te, access, True, *mda)
            slices: ArrayList[Row] | ArrayList[Column] = (
                self._rows if te.element_type == ElementType.Row else self._columns
            )
            if te._insert_slice(slices, index) is not None and bool(set_to_current):
                te.mark_current()
            successfully_created = True
            return te
        finally:
            if successfully_created and bool(fire_events):
                pass
                # TODO: fire onCreate event

    def add_row(self, a1: int | Access | None = None, *args: object) -> Row:
        return self._add_slice_dispatch(ElementType.Row, a1, *args)  # type: ignore[return-value]

    def add_column(self, a1: int | Access | None = None, *args: object) -> Column:
        return self._add_slice_dispatch(ElementType.Column, a1, *args)  # type: ignore[return-value]

    def _get_slice_dispatch(
        self,
        et: ElementType,
        slices: ArrayList[Row] | ArrayList[Column],
        a1: int | Access | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Row | Column | None:
        # look for "quick access" tokens first, they are key=value pairs in kwargs
        if kwargs:
            a1, args = self._parse_quick_action_args(Table._quick_access_map, a1, *args, **kwargs)
        if a1 is None:
            return self._get_slice(et, slices, Access.Current, True, True)  # type: ignore[return-value]
        elif isinstance(a1, int):
            return self._get_slice(et, slices, Access.ByIndex, True, True, int(a1))  # type: ignore[return-value]
        elif isinstance(a1, Access):
            return self._get_slice(et, slices, a1, True, True, *args)  # type: ignore[return-value]
        else:
            raise UnsupportedException(self, "Cannot get Row/Column from Table with these arguments")

    def _get_slice(
        self,
        et: ElementType,
        slices: ArrayList[Row] | ArrayList[Column],
        access: Access,
        create_if_sparse: bool = True,
        set_to_current: bool = True,
        *mda: Any,
    ) -> Row | Column | None:
        from . import Row, Column

        with self.lock:
            slice_index = self._calculate_index(et, False, access, *mda)
            if slice_index < 0:
                return None
            # get the requested slice
            te = slices.__getitem__(slice_index)  # te can be None here
            if te is None and bool(create_if_sparse):  # type: ignore[unreachable]
                te = Row(self) if et == ElementType.Row else Column(self)  # type: ignore[unreachable]
                te._set_index(slice_index + 1)
                slices.__setitem__(slice_index, te)  # type: ignore[assignment]
                te._mark_initialized()
            if te is not None and bool(set_to_current):
                te.mark_current()
            return te

    def get_row(self, a1: int | Access | None = None, *args: Any, **kwargs: Any) -> Row:
        return self._get_slice_dispatch(ElementType.Row, self._rows, a1, *args, **kwargs)  # type: ignore[return-value]

    def get_column(self, a1: int | Access | None = None, *args: Any, **kwargs: Any) -> Column:
        return self._get_slice_dispatch(ElementType.Column, self._columns, a1, *args, **kwargs)  # type: ignore

    def _ensure_rows_exist(self) -> None:
        for index in range(0, self.num_rows):
            if self.__rows[index] is None:
                self._get_slice(ElementType.Row, self._rows, Access.ByIndex, True, False, index + 1)

    @property
    def rows(self) -> Collection[Row]:
        self.vet_element()
        self._ensure_rows_exist()
        return tuple(sorted(self._rows))

    def _ensure_columns_exist(self) -> None:
        for index in range(0, self.num_columns):
            if self.__cols[index] is None:
                self._get_slice(ElementType.Column, self._columns, Access.ByIndex, True, False, index + 1)

    @property
    def columns(self) -> Collection[Column]:
        self.vet_element()
        self._ensure_columns_exist()
        return tuple(sorted(self._columns))

    def _sort_row_labels(self) -> None:
        from . import TableSliceElement

        self._rows.sort(key=lambda row: (row.label is None, row.label, row.index))
        TableSliceElement._reindex_slice(self._rows)

    def _sort_column_labels(self) -> None:
        from . import TableSliceElement

        self._columns.sort(key=lambda col: (col.label is None, col.label, col.index))
        TableSliceElement._reindex_slice(self._columns)

    def sort(self, elem: Row | Column) -> None:
        if isinstance(elem, Row):
            pass
        elif isinstance(elem, Column):
            pass
        else:
            raise InvalidException(self, f"Invalid sort target: {elem}")

    def add_group(self, *elems: TableElement) -> Group:
        from . import Group

        return Group(self, None, *elems)

    def get_group(self, a1: int | Access | None = None, *args: Any, **kwargs: Any) -> Group:
        from . import Group

        # look for "quick access" tokens first, they are key=value pairs in kwargs
        if kwargs:
            a1, args = self._parse_quick_action_args(Table._quick_access_map, a1, *args, **kwargs)
        if isinstance(a1, Access):
            md = args[0] if args else None
            if a1 in [Access.ByLabel, Access.ByDescription]:
                if md is None or not isinstance(md, str):
                    raise InvalidException(self, f"Invalid Group {a1.name} value: {md}")
                if a1 == Access.ByLabel and self.is_group_labels_indexed:
                    key = str(md).strip().lower()
                    return self._group_label_index.get(key, None) if key else None  # type: ignore
                else:
                    return cast(Group, self._find(list(self._groups), a1.associated_property, str(md)))
            if a1 == Access.ByIdent:
                if md is None or not isinstance(md, int):
                    raise InvalidException(self, f"Invalid Group {a1.name} value: {md}")
                target = self._ident_index.get(int(md), None)
                return target if isinstance(target, Group) else None  # type: ignore
            if a1 == Access.ByUUID:
                if md is None:
                    return None  # type: ignore
                if not isinstance(md, str) and not isinstance(md, uuid.UUID):
                    raise InvalidException(self, f"Invalid Group {a1.name} value: {md}")
                md = md if isinstance(md, uuid.UUID) else uuid.UUID(str(md))  # type: ignore[unreachable]
                return self._uuid_index.get(md, None)  # type: ignore
            if a1 == Access.ByTags:
                if not args or any(not isinstance(item, str) for item in args):
                    raise InvalidException(self, f"Invalid Group {a1.name} value: {args}")
                return self._find_tagged(list(self._groups), *cast(Tuple[str], args))  # type: ignore
            if a1 == Access.ByProperty:
                key: Property | str = md  # type: ignore[assignment]
                value = args[1] if args and len(args) > 1 else None
                if key is None or value is None:
                    raise InvalidException(self, f"Invalid Group {a1.name} key: {key}")
                return self._find(list(self._groups), key, value)  # type: ignore
            if a1 == Access.ByReference:
                if isinstance(md, Group):
                    if md.table != self:
                        raise InvalidParentException(self, md)
                    return md
        raise InvalidAccessException(self, ElementType.Group, cast(Access, a1), False, args)
