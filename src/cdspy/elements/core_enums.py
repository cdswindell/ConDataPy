"""
ElementType enum defines all components available in the cdsPy
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Tuple, Union, Dict

from enum import Enum, Flag, verify, UNIQUE

if TYPE_CHECKING:
    from . import TableElement


@verify(UNIQUE)
class BaseElementState(Flag):
    NO_FLAGS_SET = 0x0
    ENFORCE_DATATYPE_FLAG = 0x01
    READONLY_FLAG = 0x02
    SUPPORTS_NULL_FLAG = 0x04
    AUTO_RECALCULATE_FLAG = 0x08

    AUTO_RECALCULATE_DISABLED_FLAG = 0x10
    PENDING_THREAD_POOL_FLAG = 0x20
    IN_USE_FLAG = 0x40
    IS_PENDING_FLAG = 0x80

    ROW_LABELS_INDEXED_FLAG = 0x100
    COLUMN_LABELS_INDEXED_FLAG = 0x200
    CELL_LABELS_INDEXED_FLAG = 0x400
    TABLE_LABELS_INDEXED_FLAG = 0x800

    GROUP_LABELS_INDEXED_FLAG = 0x1000
    HAS_CELL_VALIDATOR_FLAG = 0x2000
    IS_DERIVED_CELL_FLAG = 0x4000
    IS_TABLE_PERSISTENT_FLAG = 0x8000

    EVENTS_NOTIFY_IN_SAME_THREAD_FLAG = 0x100000
    EVENTS_ALLOW_CORE_THREAD_TIMEOUT_FLAG = 0x200000
    PENDINGS_ALLOW_CORE_THREAD_TIMEOUT_FLAG = 0x400000

    IS_DEFAULT_FLAG = 0x1000000
    IS_DIRTY_FLAG = 0x2000000
    HAS_CELL_ERROR_MSG_FLAG = 0x4000000
    IS_AWAITING_FLAG = 0x8000000

    IS_INVALID_FLAG = 0x10000000
    IS_PROCESSED_FLAG = 0x20000000
    IS_INITIALIZING_FLAG = 0x40000000


@verify(UNIQUE)
class ElementType(Enum):
    TableContext = 1
    """A collection of Tables"""
    Table = 2
    """ a data table, consisting of Rows, Columns, and Cells, and Groups"""
    Row = 3
    """ A Table Row"""
    Column = 4
    """A Table Column"""
    Cell = 5
    """A Table Cell containing a single value"""
    Group = 6
    """A group of Table Rows, Columns, Cells or other Groups"""
    Derivation = 7
    """An algebraic formula used to calculate a cell value"""

    @property
    def nickname(self) -> str:
        if self == ElementType.Column:
            return "Col"
        else:
            return self.name

    def properties(self) -> set[Property]:
        return {p for p in Property if p.is_implemented_by(self)}

    def required_properties(self) -> set[Property]:
        return {p for p in Property if p.is_required_property and p.is_implemented_by(self)}

    def optional_properties(self) -> set[Property]:
        return {p for p in Property if not p.is_required_property and p.is_implemented_by(self)}

    def initializable_properties(self) -> set[Property]:
        s = _INITIALIZABLE_PROPERTIES.get(self, None)
        if s is None:
            s = {p for p in Property if p.is_initializable_property and self in p.value._implemented_by}
            _INITIALIZABLE_PROPERTIES[self] = s
        return s

    def read_only_properties(self) -> set[Property]:
        return {p for p in Property if p.is_read_only_property and p.is_implemented_by(self)}

    def mutable_properties(self) -> set[Property]:
        return {p for p in Property if p.is_mutable_property and p.is_implemented_by(self)}


_INITIALIZABLE_PROPERTIES: Dict[ElementType, set[Property]] = {}


class _TablePropertyInfo:
    """
    Private class used to construct Property enums. Defines the core
    characteristics of every Property along with the set of Table Element(s)
    each property is applicable to
    """

    def __init__(
        self,
        optional: bool,
        read_only: bool,
        initializable: bool,
        nickname: Optional[str] = None,
        state: Optional[BaseElementState] = None,
        *args: ElementType,
    ) -> None:
        self._optional = optional
        self._read_only = read_only
        self._initializable = initializable
        self._nickname = nickname
        self._state = state
        self._implemented_by = set(args) if args else set(ElementType)

    def __str__(self) -> str:
        optional = "optional" if self._optional else "required"
        ro = ", read-only" if self._read_only else ""
        return f"[{optional}{ro}]"


# noinspection PyPropertyDefinition
@verify(UNIQUE)
class Property(Enum):
    """
    CdsPy defines a number of characteristics, defined in this Property class,
    to control, define, and express ...
    """

    # Base element properties supported by all table elements
    Label = _TablePropertyInfo(True, False, False, "lb")
    Description = _TablePropertyInfo(True, False, False, "desc")
    Tags = _TablePropertyInfo(True, True, False, "tags")
    UUID = _TablePropertyInfo(
        True,
        True,
        False,
        "uuid",
        None,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
        ElementType.Cell,
    )
    Ident = _TablePropertyInfo(
        True,
        True,
        False,
        "id",
        None,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
    )

    # Table Element Properties(TableContext implements initializable ones)
    Precision = _TablePropertyInfo(True, False, True, "pr", None, ElementType.TableContext, ElementType.Table)

    # TableContext/Table Properties
    TokenMapper = _TablePropertyInfo(False, False, True, None, None, ElementType.TableContext)
    RowCapacityIncr = _TablePropertyInfo(False, False, True, "rci", None, ElementType.TableContext, ElementType.Table)
    ColumnCapacityIncr = _TablePropertyInfo(
        False, False, True, "cci", None, ElementType.TableContext, ElementType.Table
    )
    FreeSpaceThreshold = _TablePropertyInfo(
        False, False, True, "fst", None, ElementType.TableContext, ElementType.Table
    )
    IsAutoRecalculateDefault = _TablePropertyInfo(
        False,
        False,
        True,
        "recalc",
        BaseElementState.AUTO_RECALCULATE_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )
    IsTableLabelsIndexedDefault = _TablePropertyInfo(False, False, True, "isTLBX", None, ElementType.TableContext)
    IsRowLabelsIndexedDefault = _TablePropertyInfo(
        False,
        False,
        True,
        "isRLbX",
        BaseElementState.ROW_LABELS_INDEXED_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )
    IsColumnLabelsIndexedDefault = _TablePropertyInfo(
        False,
        False,
        True,
        "isCLbX",
        BaseElementState.COLUMN_LABELS_INDEXED_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )
    IsCellLabelsIndexedDefault = _TablePropertyInfo(
        False,
        False,
        True,
        "isClLbX",
        BaseElementState.CELL_LABELS_INDEXED_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )
    IsGroupLabelsIndexedDefault = _TablePropertyInfo(
        False,
        False,
        True,
        "isGLbX",
        BaseElementState.GROUP_LABELS_INDEXED_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )
    # PendingDerivationThreadPool Properties
    IsPendingAllowCoreThreadTimeoutDefault = _TablePropertyInfo(
        True,
        False,
        True,
        None,
        BaseElementState.PENDINGS_ALLOW_CORE_THREAD_TIMEOUT_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )
    NumPendingCorePoolThreads = _TablePropertyInfo(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    NumPendingMaxPoolThreads = _TablePropertyInfo(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    PendingThreadKeepAliveTimeout = _TablePropertyInfo(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    PendingThreadKeepAliveTimeoutUnit = _TablePropertyInfo(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    IsPendingThreadPoolEnabled = _TablePropertyInfo(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    IsTablesPersistentDefault = _TablePropertyInfo(
        False,
        False,
        True,
        "isP",
        BaseElementState.IS_TABLE_PERSISTENT_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )

    # Table Element Properties
    IsReadOnlyDefault = _TablePropertyInfo(
        True,
        False,
        True,
        "rod",
        BaseElementState.READONLY_FLAG,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    IsSupportsNullsDefault = _TablePropertyInfo(
        True,
        False,
        True,
        "snd",
        BaseElementState.SUPPORTS_NULL_FLAG,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    IsEnforceDataTypeDefault = _TablePropertyInfo(
        True,
        False,
        True,
        "edt",
        BaseElementState.ENFORCE_DATATYPE_FLAG,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    Derivation = _TablePropertyInfo(
        False,
        False,
        False,
        "fx",
        None,
        ElementType.Column,
        ElementType.Row,
        ElementType.Cell,
    )
    TimeSeries = _TablePropertyInfo(False, False, False, "tx", None, ElementType.Column, ElementType.Row)

    # Cell properties
    DataType = _TablePropertyInfo(False, False, False, "dt", None, ElementType.Column, ElementType.Cell)
    CellValue = _TablePropertyInfo(False, False, False, "v", None, ElementType.Cell)
    ErrorMessage = _TablePropertyInfo(True, False, False, "e", None, ElementType.Cell)
    Units = _TablePropertyInfo(
        True,
        False,
        True,
        "u",
        None,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    DisplayFormat = _TablePropertyInfo(
        True,
        False,
        True,
        "f",
        None,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    CellValidator = _TablePropertyInfo(
        True, False, False, "cv", None, ElementType.Row, ElementType.Column, ElementType.Cell
    )

    @classmethod
    def _missing_(cls, name: object) -> Property:
        if name is None:
            raise ValueError(f"None is not a valid {cls.__name__}")

        name = str(name).lower().strip()
        for member in cls:
            if member.name.lower() == name:
                return member
        raise ValueError(f"'{name}' is not a valid {cls.__name__}")

    @classmethod
    def by_name(cls, name: str) -> Property | None:
        orig_name = name
        name = name.strip()
        if name in cls.__members__:
            return cls[name]
        # fall back to case-insensitive s
        name = name.lower()
        for k, v in cls.__members__.items():
            if k.lower() == name:
                return v
        else:
            if name:
                raise ValueError(f"'{name}' is not a valid {cls.__name__}")
            else:
                raise ValueError(f"None/Empty is not a valid {cls.__name__}")

    @classmethod  # type: ignore[misc]
    @property
    def read_only(cls) -> Tuple[Property, ...]:
        return tuple(sorted([p for p in cls if p.is_read_only_property]))

    @classmethod  # type: ignore[misc]
    @property
    def initializable(cls) -> Tuple[Property, ...]:
        return tuple(sorted([p for p in cls if p.is_initializable_property]))

    @classmethod  # type: ignore[misc]
    @property
    def optional(cls) -> Tuple[Property, ...]:
        return tuple(sorted([p for p in cls if p.is_optional_property]))

    @classmethod  # type: ignore[misc]
    @property
    def state_default(cls) -> Tuple[Property, ...]:
        return tuple(sorted([p for p in cls if p.is_state_default_property]))

    @staticmethod
    def by_nickname(short_name: Optional[str] = None) -> Optional[Property]:
        if short_name is None or short_name.strip() is None:
            return None
        short_name = short_name.strip().lower()
        return _PROPERTIES_BY_NICKNAME[short_name] if short_name in _PROPERTIES_BY_NICKNAME else None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Property):
            return False
        return self.name == other.name

    def __lt__(self, other: Property) -> bool:
        if not isinstance(other, Property):
            raise NotImplementedError
        return self.name < other.name

    def __gt__(self, other: Property) -> bool:
        if not isinstance(other, Property):
            raise NotImplementedError
        return self.name > other.name

    def __hash__(self) -> int:
        return self.name.__hash__()

    @property
    def nickname(self) -> str:
        if self.value._nickname:
            return str(self.value._nickname)
        else:
            return self.name

    def is_implemented_by(self, e: Union[ElementType, TableElement, None]) -> bool:
        from . import TableElement

        if e is None:
            return False
        if isinstance(e, TableElement):
            return e.element_type in self.value._implemented_by
        if isinstance(e, ElementType):
            return e in self.value._implemented_by
        else:
            return False  # type: ignore

    @property
    def is_read_only_property(self) -> bool:
        return bool(self.value._read_only)

    @property
    def is_mutable_property(self) -> bool:
        return not self.value._read_only

    @property
    def is_optional_property(self) -> bool:
        return bool(self.value._optional)

    @property
    def is_required_property(self) -> bool:
        return not self.value._optional

    @property
    def is_initializable_property(self) -> bool:
        return self.value._initializable

    @property
    def is_state_default_property(self) -> bool:
        return self.value._state is not None

    @property
    def state(self) -> BaseElementState:
        return self.value._state  # type: ignore[return-value]

    @property
    def is_boolean_property(self) -> bool:
        if self.name.lower().startswith("is"):
            return True
        # handle one-offs by placing them in set
        return self in set([])

    @property
    def is_numeric_property(self) -> bool:
        if self.name.lower().startswith("num"):
            return True

        # handle one-offs by placing them in set
        return self in {
            Property.Precision,
            Property.RowCapacityIncr,
            Property.ColumnCapacityIncr,
            Property.FreeSpaceThreshold,
        }

    @property
    def is_string_property(self) -> bool:
        # handle one-offs by placing them in set
        return self in {
            Property.Label,
            Property.Description,
            Property.Units,
            Property.DisplayFormat,
        }


# Define static property maps
_PROPERTIES_BY_NICKNAME = {p.nickname.lower(): p for p in Property}


class _AccessInfo:
    """ """

    def __init__(self, p: Optional[Property] = None) -> None:
        self._associated_property = p


@verify(UNIQUE)
class Access(Enum):
    First = _AccessInfo()
    Last = _AccessInfo()
    Next = _AccessInfo()
    Previous = _AccessInfo()
    Current = _AccessInfo()
    ByIndex = _AccessInfo()
    ByReference = _AccessInfo()
    ByProperty = _AccessInfo()
    ByTags = _AccessInfo()
    ByDataType = _AccessInfo(Property.DataType)
    ByIdent = _AccessInfo(Property.Ident)
    ByLabel = _AccessInfo(Property.Label)
    ByDescription = _AccessInfo(Property.Description)
    ByUUID = _AccessInfo(Property.UUID)

    @property
    def has_associated_property(self) -> bool:
        return self.value._associated_property is not None

    @property
    def associated_property(self) -> Property:
        return self.value._associated_property  # type: ignore[return-value]


class _EventTypeInfo:
    """ """

    def __init__(self, notify_in_same_thread: bool, notify_parent: bool, *args: ElementType) -> None:
        self._notify_in_same_thread = bool(notify_in_same_thread)
        self._notify_parent = bool(notify_parent)
        self._implemented_by = set(args) if args else {}


@verify(UNIQUE)
class EventType(Enum):
    OnBeforeCreate = _EventTypeInfo(
        True, False, ElementType.Table, ElementType.Group, ElementType.Row, ElementType.Column
    )
    OnBeforeDelete = _EventTypeInfo(
        True, True, ElementType.Table, ElementType.Group, ElementType.Row, ElementType.Column
    )
    OnBeforeNewValue = _EventTypeInfo(
        True, True, ElementType.Table, ElementType.Row, ElementType.Column, ElementType.Cell
    )
    OnNewValue = _EventTypeInfo(False, True, ElementType.Table, ElementType.Row, ElementType.Column, ElementType.Cell)
    OnCreate = _EventTypeInfo(False, True, ElementType.Table, ElementType.Group, ElementType.Row, ElementType.Column)
    OnDelete = _EventTypeInfo(False, True, ElementType.Table, ElementType.Group, ElementType.Row, ElementType.Column)
    OnPendings = _EventTypeInfo(False, True, ElementType.Table, ElementType.Row, ElementType.Column, ElementType.Cell)
    OnNoPendings = _EventTypeInfo(False, True, ElementType.Table, ElementType.Row, ElementType.Column, ElementType.Cell)
    OnRecalculate = _EventTypeInfo(
        False, True, ElementType.Table, ElementType.Row, ElementType.Column, ElementType.Cell
    )

    @property
    def is_notify_parent(self) -> bool:
        return self.value._notify_parent

    @property
    def is_notify_in_same_thread(self) -> bool:
        return self.value._notify_in_same_thread

    def is_implemented_by(self, e: ElementType | TableElement) -> bool:
        from . import TableElement

        if isinstance(e, TableElement):
            e = e.element_type
        return e in self.value._implemented_by


@verify(UNIQUE)
class TimeUnit(Enum):
    MIN = 1.0 / 60.0
    SEC = 1
    MSEC = 1000
    USEC = 1000000
