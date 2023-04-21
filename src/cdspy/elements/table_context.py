from __future__ import annotations

from collections.abc import Collection, Sequence
from threading import RLock
from typing import Any, cast, Dict, Final, Iterator, Optional, Set, TYPE_CHECKING
from weakref import WeakSet, WeakValueDictionary

from . import BaseElementState
from . import BaseElement
from .base_element import _BaseElementIterable

from . import Access
from . import ElementType
from . import Property
from . import TimeUnit

from ..exceptions import InvalidException
from ..exceptions import InvalidAccessException

from ..mixins import DerivableThreadPool
from ..mixins import DerivableThreadPoolConfig
from ..mixins import EventProcessorThreadPool
from ..mixins import EventsProcessorThreadPoolCreator


if TYPE_CHECKING:
    from . import Tag
    from . import Table


_TABLE_CONTEXT_DEFAULTS: Dict[Property, Any] = {
    Property.RowCapacityIncr: 256,
    Property.ColumnCapacityIncr: 256,
    Property.FreeSpaceThreshold: 2.0,
    Property.IsAutoRecalculateDefault: True,
    Property.DisplayFormat: None,
    Property.Units: None,
    Property.Precision: None,
    Property.IsReadOnlyDefault: False,
    Property.IsSupportsNullsDefault: True,
    Property.IsEnforceDataTypeDefault: False,
    Property.IsTableLabelsIndexedDefault: False,
    Property.IsRowLabelsIndexedDefault: False,
    Property.IsColumnLabelsIndexedDefault: False,
    Property.IsCellLabelsIndexedDefault: False,
    Property.IsGroupLabelsIndexedDefault: False,
    Property.IsTablesPersistentDefault: False,
    Property.IsGroupsPersistentDefault: False,
    Property.IsPendingThreadPoolEnabled: True,
    Property.IsPendingAllowCoreThreadTimeoutDefault: True,
    Property.NumPendingCorePoolThreads: 8,
    Property.NumPendingMaxPoolThreads: 128,
    Property.PendingThreadKeepAliveTimeout: 5,
    Property.PendingThreadKeepAliveTimeoutUnit: TimeUnit.SEC,
}

_EVENTS_NOTIFY_IN_SAME_THREAD_DEFAULT: Final[bool] = False
_EVENTS_CORE_POOL_SIZE_DEFAULT: Final[int] = 2
_EVENTS_MAX_POOL_SIZE_DEFAULT: Final[int] = 5
_EVENTS_KEEP_ALIVE_TIMEOUT_SEC_DEFAULT: Final[int] = 30
_EVENTS_ALLOW_CORE_THREAD_TIMEOUT_DEFAULT: Final[bool] = False


class TableContext(
    BaseElement,
    DerivableThreadPool,
    DerivableThreadPoolConfig,
    EventProcessorThreadPool,
    EventsProcessorThreadPoolCreator,
):
    _class_lock = RLock()

    def __new__(cls, template_context: Optional[TableContext] = None) -> TableContext:
        with cls._class_lock:
            if template_context is not None:
                return super().__new__(cls)
            # Another thread could have created the instance
            # before we acquired the lock. So check that the
            # instance is still nonexistent.
            if not hasattr(cls, "_default_table_context"):
                cls._default_table_context = super().__new__(cls)
        return cls._default_table_context

    @staticmethod
    def fetch_default_context() -> TableContext:
        return TableContext()

    @staticmethod
    def create_context(template: Optional[TableContext] = None) -> TableContext:
        if template is None:
            return TableContext()
        else:
            return TableContext(template)

    @staticmethod
    def _make_table_label_key(label: str) -> str | None:
        if label:
            return " ".join(label.strip().lower().split())
        else:
            return None

    def __init__(self, template: Optional[TableContext] = None) -> None:
        # we want to prevent __init__ being called on this class instance more than once,
        # which could occur from our singleton-like __new__ method
        if hasattr(self, "_sealed") and self._sealed:  # type: ignore[has-type]
            return
        super().__init__()
        self._lock = RLock()

        self._registered_nonpersistent_tables: WeakSet[Table] = WeakSet()
        self._registered_persistent_tables: Set[Table] = set()

        self._mutate_state(BaseElementState.IS_DEFAULT_FLAG, False if template else True)

        global _TABLE_CONTEXT_DEFAULTS
        for p in self.element_type.initializable_properties():
            v = template.get_property(p) if template else _TABLE_CONTEXT_DEFAULTS.get(p, None)
            self._initialize_property(p, v)

        if not template:
            self.label = "Default Table Context"

        # table label map
        self._table_label_map: WeakValueDictionary[str, Table] = WeakValueDictionary()
        self._mark_initialized()
        self._sealed = True

    def __iter__(self) -> Iterator[Table]:
        from . import Table

        return _BaseElementIterable[Table](self.tables)

    def __len__(self) -> int:
        return self.num_tables

    @property
    def is_table_labels_indexed(self) -> bool:
        return self._is_set(BaseElementState.TABLE_LABELS_INDEXED_FLAG)

    @is_table_labels_indexed.setter
    def is_table_labels_indexed(self, state: bool) -> None:
        with self.lock:
            if bool(state):
                self._index_all_table_labels()
            else:
                self._table_label_map.clear()
            self._mutate_state(BaseElementState.TABLE_LABELS_INDEXED_FLAG, state)

    def _index_all_table_labels(self) -> None:
        for t in self.tables:
            try:
                self._index_table_label(t, True)
            except KeyError as e:
                self.is_table_labels_indexed = False
                raise e

    def _index_table_label(self, t: Table, force_it: bool = False) -> None:
        key = TableContext._make_table_label_key(t.label)
        if key and (self.is_table_labels_indexed or force_it):
            with self.lock:
                if key in self._table_label_map and self._table_label_map[key] != t:
                    raise KeyError(f"TableContext: Table label '{key}' not unique")
                else:
                    self._table_label_map[key] = t

    @property
    def free_space_threshold_default(self) -> float:
        return cast(float, self.get_property(Property.FreeSpaceThreshold))

    @free_space_threshold_default.setter
    def free_space_threshold_default(self, default: float) -> None:
        self._set_property(Property.FreeSpaceThreshold, default)

    @property
    def row_capacity_incr_default(self) -> int:
        return cast(int, self.get_property(Property.RowCapacityIncr))

    @row_capacity_incr_default.setter
    def row_capacity_incr_default(self, default: int) -> None:
        self._set_property(Property.RowCapacityIncr, default)

    @property
    def column_capacity_incr_default(self) -> int:
        return cast(int, self.get_property(Property.ColumnCapacityIncr))

    @column_capacity_incr_default.setter
    def column_capacity_incr_default(self, default: int) -> None:
        self._set_property(Property.ColumnCapacityIncr, default)

    @property
    def is_tables_persistent_default(self) -> bool:
        return cast(bool, self.get_property(Property.IsTablesPersistentDefault))

    @is_tables_persistent_default.setter
    def is_tables_persistent_default(self, default: bool) -> None:
        self._set_property(Property.IsTablesPersistentDefault, default)

    @property
    def is_groups_persistent_default(self) -> bool:
        return cast(bool, self.get_property(Property.IsGroupsPersistentDefault))

    @is_groups_persistent_default.setter
    def is_groups_persistent_default(self, default: bool) -> None:
        self._set_property(Property.IsGroupsPersistentDefault, default)

    @property
    def is_auto_recalculate_default(self) -> bool:
        return cast(bool, self.get_property(Property.IsAutoRecalculateDefault))

    @is_auto_recalculate_default.setter
    def is_auto_recalculate_default(self, default: bool) -> None:
        self._set_property(Property.IsAutoRecalculateDefault, default)

    @property
    def display_format_default(self) -> str:
        return cast(str, self.get_property(Property.DisplayFormat))

    @display_format_default.setter
    def display_format_default(self, default: str) -> None:
        self._set_property(Property.DisplayFormat, default)

    @property
    def units_default(self) -> str:
        return cast(str, self.get_property(Property.Units))

    @units_default.setter
    def units_default(self, default: str) -> None:
        self._set_property(Property.Units, default)

    @property
    def tables(self) -> Sequence[Table]:
        return tuple(sorted(set(self._registered_persistent_tables) | set(self._registered_nonpersistent_tables)))

    @property
    def labeled_tables(self) -> Dict[str, Table]:
        return {t.label: t for t in self.tables if t.label}

    @property
    def lock(self) -> RLock:
        return self._lock

    @property
    def is_null(self) -> bool:
        return self.num_tables == 0

    @property
    def element_type(self) -> ElementType:
        return ElementType.TableContext

    @property
    def is_default(self) -> bool:
        return self._is_set(BaseElementState.IS_DEFAULT_FLAG)

    @property
    def num_tables(self) -> int:
        with self.lock:
            return len(self._registered_nonpersistent_tables) + len(self._registered_persistent_tables)

    def create_table(self, *args: Any, **kwargs: Any) -> Table:
        from . import Table

        num_rows = self._parse_args(int, "num_rows", 0, self.row_capacity_incr_default, *args, **kwargs)
        num_cols = self._parse_args(int, "num_cols", 1, self.column_capacity_incr_default, *args, **kwargs)
        label = self._parse_args(str, "label", None, None, *args, **kwargs)
        description = self._parse_args(str, "description", None, None, *args, **kwargs)
        units = self._parse_args(str, "units", None, None, *args, **kwargs)
        display_format = self._parse_args(str, "display_format", None, None, *args, **kwargs)
        t = Table(num_rows, num_cols, self)
        if label:
            t.label = label
        if description:
            t.description = description
        if units:
            t.units = units
        if display_format:
            t.display_format = display_format
        return t

    def clear(self) -> None:
        with self.lock:
            self._table_label_map.clear()
            while self._registered_persistent_tables:
                t = self._registered_persistent_tables.pop()
                if t and t.is_valid:
                    t._delete()
                    del t

            while self._registered_nonpersistent_tables:
                t = self._registered_nonpersistent_tables.pop()
                if t and t.is_valid:
                    t._delete()
                    del t

    def _purge_labeled_table(self, t: Table) -> None:
        if t is not None:
            key = TableContext._make_table_label_key(t.label)
            with self.lock:
                if key and key in self._table_label_map:
                    self._table_label_map.pop(key)

    def _register(self, t: Table) -> Optional[TableContext]:
        if t:
            with self.lock:
                if t.is_persistent:
                    self._registered_nonpersistent_tables.discard(t)
                    self._registered_persistent_tables.add(t)
                else:
                    self._registered_persistent_tables.discard(t)
                    self._registered_nonpersistent_tables.add(t)
                self._index_table_label(t)
        return self

    def _deregister(self, t: Table) -> None:
        if t:
            with self.lock:
                self._purge_labeled_table(t)
                self._registered_nonpersistent_tables.discard(t)
                self._registered_persistent_tables.discard(t)

    def is_registered(self, t: Table) -> bool:
        return t in self._registered_persistent_tables or t in self._registered_nonpersistent_tables

    @property
    def is_datatype_enforced(self) -> bool:
        return self.is_enforce_datatype

    @property
    def _tags(self) -> Dict[str, Tag] | None:
        from . import Tag

        return cast(Dict[str, Tag], self.get_property(Property.Tags))

    @property
    def tags(self) -> Collection[str] | None:
        with self.lock:
            tags = self._tags
            return sorted([str(k) for k in tags.keys()]) if tags else []

    def to_canonical_tag(self, label: str, create: Optional[bool] = True) -> Optional[Tag]:
        from . import Tag

        label = Tag.normalize_label(label)
        if label:
            with self.lock:
                tags = self._tags
                if tags is None:
                    tags = {}
                    self._initialize_property(Property.Tags, tags)
                # do we know about this tag?
                tag = tags.get(label, None)
                if not tag and create:
                    tag = Tag(label)
                    tags[label] = tag
                return tag
        return None

    def get_table(self, mode: Access, *args: object) -> Table | None:
        from . import Table

        if mode.has_associated_property:
            if args:
                if self.is_table_labels_indexed and (mode.associated_property == Property.Label):
                    key = TableContext._make_table_label_key(str(args[0]))
                    if key and key in self._table_label_map:
                        return self._table_label_map[key]
                    else:
                        return None
                return BaseElement._find(self.tables, mode.associated_property, args[0])  # type: ignore[return-value]
            raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {args}")
        elif mode == Access.ByTags:
            if args and str in {type(t) for t in args}:
                return BaseElement._find_tagged(self.tables, *[v for v in args if v and isinstance(v, str)])  # type: ignore[return-value]
            raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {args}")
        elif mode == Access.ByProperty:
            pkey = cast(Property, args[0] if args and len(args) > 0 else None)
            value = args[1] if args and len(args) > 1 else None
            if pkey:
                return BaseElement._find(self.tables, pkey, value)  # type: ignore[return-value]
            raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {value}")
        elif mode == Access.ByReference:
            if args:
                t = cast(BaseElement, args[0])
                if not t or not isinstance(t, Table) or t.element_type != ElementType.Table:
                    raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {t}")
                self.vet_element(t)
                return t
        raise InvalidAccessException(self, ElementType.Table, mode, False, args)
