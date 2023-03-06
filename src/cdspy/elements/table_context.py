from __future__ import annotations

from threading import Lock
from typing import Dict, Final, Optional, Set
from weakref import WeakSet

from . import BaseElement
from . import BaseElementState
from . import ElementType
from . import Property
from . import TimeUnit
from . import Tag

_TABLE_CONTEXT_DEFAULTS = {
    Property.RowCapacityIncr: 256,
    Property.ColumnCapacityIncr: 256,
    Property.FreeSpaceThreshold: 2.0,
    Property.IsAutoRecalculate: True,
    Property.DisplayFormat: None,
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


class TableContext(BaseElement):
    _default_table_context: Optional[TableContext] = None
    _lock = Lock()

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
    def default_table_context(cls) -> TableContext:
        return cls.__new__(cls)

    def __init__(self, template_context: Optional[TableContext] = None) -> None:
        super().__init__()

        is_default = True if template_context is None else False

        self._mutate_flag(BaseElementState.IS_DEFAULT_FLAG, is_default)
        self._m_registered_nonpersistent_tables: WeakSet[BaseElement] = WeakSet()
        self._m_registered_persistent_tables: Set[BaseElement] = set()
        self._m_global_tag_cache: Dict[str, Tag] = {}

        # initialize all initializable properties
        self._initialize(template_context)

    def _initialize(self, template: Optional[TableContext] = None) -> None:
        global _TABLE_CONTEXT_DEFAULTS
        for p in self.element_type.initializable_properties():
            v = template.get_property(p) if template else None
            if v is None:
                v = _TABLE_CONTEXT_DEFAULTS.get(p, None)
            self._set_property(p, v)

    @property
    def _is_null(self) -> bool:
        return not self._m_registered_nonpersistent_tables and not self._m_registered_persistent_tables

    @property
    def element_type(self) -> ElementType:
        return ElementType.TableContext

    @property
    def is_default(self) -> bool:
        return self._is_set(BaseElementState.IS_DEFAULT_FLAG)

    @property
    def num_tables(self) -> int:
        return len(self._m_registered_nonpersistent_tables) + len(self._m_registered_persistent_tables)
