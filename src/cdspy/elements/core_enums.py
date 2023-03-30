"""
ElementType enum defines all components available in the cdsPy
"""
from __future__ import annotations

import re
from typing import Optional, TYPE_CHECKING, Union

from enum import Enum, verify, UNIQUE

if TYPE_CHECKING:
    from . import TableElement


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

    def properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self)})

    def required_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_required_property})

    def optional_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and not p.is_required_property})

    def initializable_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_initializable_property})

    def read_only_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_read_only_property})

    def mutable_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_mutable_property})


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
        state: Optional[bool] = False,
        *args: ElementType,
    ) -> None:
        self._optional = optional
        self._read_only = read_only
        self._initializable = initializable
        self._nickname = nickname
        self._state = bool(state)
        self._implemented_by = set(args) if args else set(ElementType)

    def __str__(self) -> str:
        optional = "optional" if self._optional else "required"
        ro = ", read-only" if self._read_only else ""
        return f"[{optional}{ro}]"


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
    IsAutoRecalculate = _TablePropertyInfo(
        False, False, True, "recalc", None, ElementType.TableContext, ElementType.Table
    )
    IsTableLabelsIndexed = _TablePropertyInfo(False, False, True, "isTLBX", None, ElementType.TableContext)
    IsRowLabelsIndexed = _TablePropertyInfo(
        False, False, True, "isRLbX", None, ElementType.TableContext, ElementType.Table
    )
    IsColumnLabelsIndexed = _TablePropertyInfo(
        False, False, True, "isCLbX", None, ElementType.TableContext, ElementType.Table
    )
    IsCellLabelsIndexed = _TablePropertyInfo(
        False, False, True, "isClLbX", None, ElementType.TableContext, ElementType.Table
    )
    IsGroupLabelsIndexed = _TablePropertyInfo(
        False, False, True, "isGLbX", None, ElementType.TableContext, ElementType.Table
    )
    # PendingDerivationThreadPool Properties
    IsPendingAllowCoreThreadTimeout = _TablePropertyInfo(
        True,
        False,
        True,
        None,
        None,
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
    AreTablesPersistentDefault = _TablePropertyInfo(
        False, False, True, "isP", True, ElementType.TableContext, ElementType.Table
    )

    # Table Element Properties
    IsReadOnlyDefault = _TablePropertyInfo(
        True,
        False,
        True,
        "rod",
        True,
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
        True,
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
        True,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    NumRowsCapacity = _TablePropertyInfo(False, True, False, None, None, ElementType.Table)
    NumColumnsCapacity = _TablePropertyInfo(False, True, False, None, None, ElementType.Table)
    NumCellsCapacity = _TablePropertyInfo(False, True, False, None, None, ElementType.Column)
    NextCellOffset = _TablePropertyInfo(False, True, False, None, None, ElementType.Table)
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
    Row = _TablePropertyInfo(False, True, False, None, None, ElementType.Cell)
    Column = _TablePropertyInfo(False, True, False, None, None, ElementType.Cell)
    CellOffset = _TablePropertyInfo(False, True, False, None, None, ElementType.Row, ElementType.Cell)
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

    @classmethod
    def by_attr_name(cls, name: str) -> Optional[Property]:
        try:
            return Property.by_name("".join(name.title().split("_")))
        except ValueError:
            return None

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
    def as_attr_name(self) -> str:
        return "_".join([t.lower() for t in re.split("(?<=.)(?=[A-Z])", self.name)])

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
        if isinstance(e, ElementType):
            return e in self.value._implemented_by
        if isinstance(e, TableElement):
            return self.is_implemented_by(e.element_type)
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
        return bool(self.value._state)

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
            Property.CellOffset,
            Property.NextCellOffset,
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


# Define static Nickname map
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
