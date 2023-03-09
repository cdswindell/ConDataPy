from __future__ import annotations

from collections.abc import Collection
from threading import RLock
from typing import Any, cast, Dict, Final, Optional, Set, TYPE_CHECKING
from weakref import WeakSet

from . import BaseElementState
from . import BaseElement

from . import Access
from . import ElementType
from . import Property
from . import TimeUnit

from ..mixins import DerivableThreadPool
from ..mixins import DerivableThreadPoolConfig
from ..mixins import EventProcessorThreadPool
from ..mixins import EventsProcessorThreadPoolCreator

if TYPE_CHECKING:
    from . import Tag

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

        self._registered_nonpersistent_tables: WeakSet[BaseElement] = WeakSet()
        self._registered_persistent_tables: Set[BaseElement] = set()

        self._mutate_flag(BaseElementState.IS_DEFAULT_FLAG, template is None)

        global _TABLE_CONTEXT_DEFAULTS
        for p in self.element_type.initializable_properties():
            v = template.get_property(p) if template else None
            if v is None:
                v = _TABLE_CONTEXT_DEFAULTS.get(p, None)
            self._initialize_property(p, v)

    @property
    def _is_null(self) -> bool:
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
        # delete the persistent tables
        for t in self._registered_persistent_tables:
            if t.is_valid:
                t._delete()

        self._registered_persistent_tables.clear()
        self._registered_nonpersistent_tables.clear()

    @property
    def _tags(self) -> Dict[str, Tag] | None:
        return cast(Dict[str, Tag], self.get_property(Property.Tags))

    @property
    def tags(self) -> Collection[str] | None:
        with self.lock:
            tags = self._tags
            return sorted([str(k) for k in tags.keys()]) if tags else None

    def to_cononical_tag(self, label: str, create: Optional[bool] = True) -> Optional[Tag]:
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
        pass
