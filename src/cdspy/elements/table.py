from __future__ import annotations

from collections import deque
import threading
import weakref

from typing import Any, cast, Dict, Iterator, Optional, overload, Collection, TYPE_CHECKING, Tuple

import uuid

from ..utils import ArrayList
from ..events import BlockedRequestException
from ..exceptions import InvalidException
from ..exceptions import UnsupportedException
from ..exceptions import InvalidAccessException

from . import Access
from . import BaseElementState
from . import BaseElement
from . import ElementType
from . import Property
from . import TableElement
from . import TableCellsElement
from . import TableContext

from .base_element import _BaseElementIterable

from ..computation import recalculate_affected

from ..mixins import Derivable

if TYPE_CHECKING:
    from . import T
    from . import TableSliceElement
    from . import Row
    from . import Column
    from . import Cell
    from . import Group

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


class Table(TableCellsElement):
    _table_class_lock = threading.RLock()

    @classmethod
    def table_class_lock(cls) -> threading.RLock:
        return cls._table_class_lock

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        from . import Row
        from . import Column

        super().__init__(None)

        num_rows = self._parse_args(int, "num_rows", 0, TableContext().row_capacity_incr, *args, **kwargs)
        num_cols = self._parse_args(int, "num_cols", 1, TableContext().column_capacity_incr, *args, **kwargs)
        parent_context = self._parse_args(TableContext, "parent_context", None, None, *args, **kwargs)
        template_table = self._parse_args(Table, "template_table", None, None, *args, **kwargs)

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
        self.__next_cell_offset = 0

        self._rows_capacity = self._calculate_rows_capacity(num_rows)
        self._columns_capacity = self._calculate_columns_capacity(num_cols)

        self._row_label_index: Dict[str, Row] = {}
        self._col_label_index: Dict[str, Column] = {}
        self._cell_label_index: Dict[str, Cell] = {}
        self._Group_label_index: Dict[str, Group] = {}

        self.__table_creation_thread = weakref.ref(threading.current_thread())

        # finally, register table with context
        parent_context._register(self)

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
                super()._delete(compress)
            finally:
                self._clear_current_cell()
                self._invalidate()
                if self.table_context:
                    self.table_context._deregister(self)
                    self._context = cast(TableContext, None)

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
            self.__next_cell_offset += 1
            return self.__next_cell_offset - 1

    def _map_cell_offset_to_row(self, row: Row) -> None:
        if row and row._cell_offset >= 0:
            self._cell_offset_row_map[row._cell_offset] = row

    def _row_by_cell_offset(self, offset: int) -> Row:
        try:
            return self._cell_offset_row_map[offset]
        except KeyError:
            return cast(Row, None)

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
        capacity = cast(int, self.row_capacity_incr)
        if num_required > 0:
            remainder = num_required % capacity
            capacity = num_required + (capacity - remainder if remainder > 0 else 0)
        return capacity

    def _calculate_columns_capacity(self, num_required: int) -> int:
        capacity = cast(int, self.column_capacity_incr)
        if num_required > 0:
            remainder = num_required % capacity
            capacity = num_required + (capacity - remainder if remainder > 0 else 0)
        return capacity

    # noinspection DuplicatedCode
    def _reclaim_column_space(self) -> None:
        if len(self.__cols) == 0:
            self._cell_offset_row_map.clear()
            self._unused_cell_offsets.clear()
            if self.__next_cell_offset > 0:
                for r in self._rows:
                    r._set_cell_offset(-1)
            self.__next_cell_offset = 0

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
            if self.__next_cell_offset > 0:
                for c in self._columns:
                    c._reclaim_cell_space(self._rows, 0)
            self.__next_cell_offset = 0

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
                c._reclaim_cell_space(self._rows, num_rows)
            if self.__next_cell_offset > 0:
                self._cell_offset_row_map.clear()
                cell_offset = 0
                if num_rows > 0:
                    for r in self._rows:
                        if r and r._cell_offset >= 0:
                            r._set_cell_offset(cell_offset if num_cols else -1)
                            cell_offset += 1
                self._unused_cell_offsets.clear()
                self.__next_cell_offset = cell_offset

    def get_cell(self, row: Row, col: Column) -> Cell:
        return self._get_cell(row, col, True)  # type: ignore[return-value]

    def is_cell(self, row: Row, col: Column) -> bool:
        return self._get_cell(row, col, False) is not None

    def get_cell_value(self, row: Row, col: Column, do_format: bool = False) -> Any:
        cell = self._get_cell(row, col, False)
        if cell:
            if bool(do_format):
                return cell.formatted_cell_value
            else:
                return cell.cell_value
        else:
            return None

    def get_formatted_cell_value(self, row: Row, col: Column) -> Any:
        cell = self._get_cell(row, col, False)
        if cell:
            return cell.formatted_cell_value
        else:
            return None

    def _get_cell(self, row: Row, col: Column, create_if_sparse: bool = True) -> Cell | None:
        if (row is None) or (col is None):
            return None  # type: ignore[unreachable]
        self.vet_parent(row, col)
        with self.lock:
            return col._get_cell(row, create_if_sparse=create_if_sparse, set_to_current=True)

    def _get_cell_affects(self, cell: Cell, include_indirects: Optional[bool] = True) -> Collection[Derivable]:
        return []

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

    @property
    def is_datatype_enforced(self) -> bool:
        if self.is_enforce_datatype:
            return True
        return self.table_context.is_enforce_datatype if self.table_context else False

    @property
    def is_nulls_supported(self) -> bool:
        if self.is_supports_null:
            return True
        return self.table_context.is_supports_null if self.table_context else False

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
        return 0

    @property
    def are_cell_labels_indexed(self) -> bool:
        return False

    def fill(self, o: Optional[object]) -> None:
        pass

    def clear(self) -> None:
        self.fill(None)

    @property
    def is_label_indexed(self) -> bool:
        return False

    @property
    def derived_elements(self) -> Collection[Derivable]:
        return []

    # override to
    @BaseElement.is_persistent.setter  # type: ignore
    def is_persistent(self, state: bool) -> None:
        with self.lock:
            self._mutate_state(BaseElementState.IS_TABLE_PERSISTENT_FLAG, state)  # type: ignore
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
                    _THREAD_LOCAL_TABLE_STORAGE._current_cell_map = weakref.WeakKeyDictionary[Table, _CellReference]()
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
                    _THREAD_LOCAL_TABLE_STORAGE._current_cell_stack = weakref.WeakKeyDictionary[
                        Table, deque[_CellReference]
                    ]()
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
        if access == Access.First:
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
        elif access == Access.ByIndex:
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
            target = self._find(slices, Property.Ident, int(md))
            return int(target.index) - 1 if target else -1
        elif access == Access.ByReference:
            if is_adding or md is None or not isinstance(md, TableSliceElement) or md.element_type != et:
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            return int(md.index) - 1
        elif access in [Access.ByLabel, Access.ByDescription]:
            if is_adding or md is None or not isinstance(md, str):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            target = self._find(slices, access.associated_property, str(md))
            return int(target.index) - 1 if target else -1
        elif access == Access.ByUUID:
            if is_adding or md is None or (not isinstance(md, str) and not isinstance(md, uuid.UUID)):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {md}")
            md = md if isinstance(md, uuid.UUID) else uuid.UUID(str(md))  # type: ignore[unreachable]
            target = self._find(slices, Property.UUID, md)
            return int(target.index) - 1 if target else -1
        elif access == Access.ByTags:
            if is_adding or not mda or any(not isinstance(item, str) for item in mda):
                raise InvalidException(self, f"Invalid {et.name} {access.name} value: {mda}")
            target = self._find_tagged(slices, *cast(Tuple[str], mda))
            return int(target.index) - 1 if target else -1
        elif access == Access.ByProperty:
            key: Property | str = md  # type: ignore[assignment]
            value = mda[1] if mda and len(mda) > 1 else None
            if is_adding or key is None or value is None:
                raise InvalidException(self, f"Invalid {et.name} {access.name} key: {key}")
            target = self._find(slices, key, value)
            return int(target.index) - 1 if target else -1

        return -1

    def _add_slice_dispatch(
        self, et: ElementType, a1: int | Access | None = None, *args: object
    ) -> Row | Column | None:
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
                    raise InvalidException(self, f"{slice_type.name} with {access.name} {mda[0]} exists")
            insert_mode = Access.Last  # add new slice as last

        # create a new row/column object and insert it into table
        with self.lock:
            raw_te: Row | Column = Row(self) if slice_type == ElementType.Row else Column(self)
            te = self.__add(raw_te, insert_mode, set_to_current, True, *mda)
            # do post_processing
            if te and mda:
                if access == Access.ByLabel:
                    te.label = str(mda[0])
                elif access == Access.ByUUID:
                    te.uuid = mda[0] if isinstance(mda[0], uuid.UUID) else uuid.UUID(str(mda[0]))
                elif access == Access.ByDescription:
                    te.description = str(mda[0])
            return te  # type: ignore[return-value]

    def __add(
        self, te: Row | Column, access: Access, set_to_current: bool = True, fire_events: bool = True, *mda: object
    ) -> None | Row | Column:
        self.vet_element()
        with self.lock:
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
        self, et: ElementType, slices: ArrayList[Row] | ArrayList[Column], a1: int | Access | None = None, *args: object
    ) -> Row | Column | None:
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
        *mda: object,
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

    def get_row(self, a1: int | Access | None = None, *args: object) -> Row:
        return self._get_slice_dispatch(ElementType.Row, self._rows, a1, *args)  # type: ignore[return-value]

    def get_column(self, a1: int | Access | None = None, *args: object) -> Column:
        return self._get_slice_dispatch(ElementType.Column, self._columns, a1, *args)  # type: ignore[return-value]

    def _ensure_rows_exist(self) -> None:
        for index in range(0, self.num_rows):
            if self.__rows[index] is None:
                self._get_slice(ElementType.Row, self._rows, Access.ByIndex, True, False, index + 1)

    @property
    def rows(self) -> Iterator[T]:
        from . import Row

        self._ensure_rows_exist()
        return _BaseElementIterable[Row](self.__rows)

    def _ensure_columns_exist(self) -> None:
        for index in range(0, self.num_columns):
            if self.__cols[index] is None:
                self._get_slice(ElementType.Column, self._columns, Access.ByIndex, True, False, index)

    @property
    def columns(self) -> Iterator[T]:
        from . import Column

        self._ensure_columns_exist()
        return _BaseElementIterable[Column](self.__cols)
