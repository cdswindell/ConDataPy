from __future__ import annotations

from collections.abc import Collection
from threading import RLock
from typing import Any, cast, Dict, Final, Iterator, Optional, Set, TYPE_CHECKING
from weakref import WeakSet

from . import BaseElementState
from . import BaseElement
from . import _BaseElementIterable

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
    from . import T
    from . import Tag
    from . import Table


_TABLE_CONTEXT_DEFAULTS: Dict[Property, Any] = {
    Property.RowCapacityIncr: 256,
    Property.ColumnCapacityIncr: 256,
    Property.FreeSpaceThreshold: 2.0,
    Property.IsAutoRecalculate: True,
    Property.DisplayFormat: None,
    Property.Units: None,
    Property.Precision: None,
    Property.IsReadOnlyDefault: False,
    Property.IsSupportsNullsDefault: True,
    Property.IsEnforceDataTypeDefault: False,
    Property.IsTableLabelsIndexed: False,
    Property.IsRowLabelsIndexed: False,
    Property.IsColumnLabelsIndexed: False,
    Property.IsCellLabelsIndexed: False,
    Property.IsGroupLabelsIndexed: False,
    Property.AreTablesPersistent: False,
    Property.IsPendingThreadPoolEnabled: True,
    Property.IsPendingAllowCoreThreadTimeout: True,
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
    _default_table_context: Optional[TableContext] = None
    _lock = RLock()

    def __new__(cls, template_context: Optional[TableContext] = None) -> TableContext:
        if template_context is not None:
            return super().__new__(cls)
        with cls._lock:
            # Another thread could have created the instance
            # before we acquired the lock. So check that the
            # instance is still nonexistent.
            if not cls._default_table_context:
                cls._default_table_context = super().__new__(cls)
                cls._default_table_context.label = "Default Table Context"
        return cls._default_table_context

    @classmethod
    def generate_default_table_context(cls) -> TableContext:
        return cls()

    def __init__(self, template: Optional[TableContext] = None) -> None:
        super().__init__()

        self._registered_nonpersistent_tables: WeakSet[Table] = WeakSet()
        self._registered_persistent_tables: Set[Table] = set()

        self._mutate_flag(BaseElementState.IS_DEFAULT_FLAG, template is None)

        global _TABLE_CONTEXT_DEFAULTS
        for p in self.element_type.initializable_properties():
            v = template.get_property(p) if template else None
            if v is None:
                v = _TABLE_CONTEXT_DEFAULTS.get(p, None)
            self._initialize_property(p, v)

    def __iter__(self) -> Iterator[T]:
        return iter(_BaseElementIterable(self.tables))

    @property
    def __all_tables(self) -> Collection[Table]:
        return set(self._registered_persistent_tables) | set(self._registered_nonpersistent_tables)

    @property
    def tables(self) -> Collection[Table]:
        return tuple(sorted(self.__all_tables))

    @property
    def is_null(self) -> bool:
        return not self._registered_nonpersistent_tables and not self._registered_persistent_tables

    @property
    def element_type(self) -> ElementType:
        return ElementType.TableContext

    @property
    def is_default(self) -> bool:
        return self._is_set(BaseElementState.IS_DEFAULT_FLAG)

    @property
    def num_tables(self) -> int:
        return len(self._registered_nonpersistent_tables) + len(self._registered_persistent_tables)

    def clear(self) -> None:
        with self.lock:
            while self._registered_persistent_tables:
                t = self._registered_persistent_tables.pop()
                if t and t.is_valid:
                    t._delete()

            while self._registered_nonpersistent_tables:
                t = self._registered_nonpersistent_tables.pop()
                if t and t.is_valid:
                    t._delete()

    def _register(self, t: Table) -> TableContext:
        if t:
            with self.lock:
                if t.is_persistent:
                    self._registered_nonpersistent_tables.discard(t)
                    self._registered_persistent_tables.add(t)
                else:
                    self._registered_persistent_tables.discard(t)
                    self._registered_nonpersistent_tables.add(t)
        return self

    def _deregister(self, t: Table) -> None:
        if t:
            with self.lock:
                self._registered_nonpersistent_tables.discard(t)
                self._registered_persistent_tables.discard(t)

    def is_registered(self, t: Table) -> bool:
        return t in self._registered_persistent_tables or t in self._registered_nonpersistent_tables

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

    def get_table(self, mode: Access, *args: object) -> Optional[BaseElement]:
        from . import Table

        if mode.has_associated_property:
            if args:
                return BaseElement._find(self.tables, cast(Property, mode.associated_property), args[0])
            raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {args}")
        elif mode == Access.ByTags:
            if args and str in {type(t) for t in args}:
                return BaseElement._find_tagged(self.tables, *[v for v in args if v and isinstance(v, str)])
            raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {args}")
        elif mode == Access.ByProperty:
            key = cast(Property, args[0]) if args and len(args) > 0 else None
            value = args[1] if args and len(args) > 1 else None
            if key:
                return BaseElement._find(self.tables, key, value)
            raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {value}")
        elif mode == Access.ByReference:
            if args:
                t = cast(BaseElement, args[0])
                if not t or not isinstance(t, Table) or t.element_type != ElementType.Table:
                    raise InvalidException(self.element_type, f"Invalid Table {mode.name} argument: {t}")
                self._vet_element(t)
                return t
        raise InvalidAccessException(self, ElementType.Table, mode, False, args)
