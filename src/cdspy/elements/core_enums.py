"""
ElementType enum defines all components available in the cdsPy
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Union

from enum import Enum, verify, UNIQUE, Flag

if TYPE_CHECKING:
    from .base_element import BaseElement


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

    def as_reference_label(self) -> str:
        if self == ElementType.Column:
            return "Col"
        else:
            return self.name

    def properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self)})

    def required_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_required()})

    def optional_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and not p.is_required()})

    def initializable_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_initializable()})

    def read_only_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_read_only()})

    def mutable_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.is_mutable()})


@verify(UNIQUE)
class BaseElementState(Flag):
    NO_FLAGS = 0x0

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


class _TableProperty:
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
        tag: Optional[str] = None,
        delegated_state: Optional[BaseElementState] = None,
        *args: ElementType,
    ) -> None:
        self._optional = optional
        self._read_only = read_only
        self._initializable = initializable
        self._tag = tag
        self._delegated_state = delegated_state
        self._implemented_by = set(args) if args else set(ElementType)

    def __repr__(self) -> str:
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
    Label = _TableProperty(True, False, False, "lb")
    Description = _TableProperty(True, False, False, "desc")
    Tags = _TableProperty(True, False, False, "tags")
    IsReadOnly = _TableProperty(False, False, True, "ro", BaseElementState.READONLY_FLAG)

    IsSupportsNull = _TableProperty(
        False,
        False,
        True,
        "isNulls",
        BaseElementState.SUPPORTS_NULL_FLAG,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    UUID = _TableProperty(
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
    Ident = _TableProperty(
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
    Context = _TableProperty(
        False,
        True,
        False,
        None,
        None,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
        ElementType.Group,
    )
    Table = _TableProperty(
        False,
        True,
        False,
        None,
        None,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
        ElementType.Group,
    )
    Precision = _TableProperty(True, False, True, "pr", None, ElementType.TableContext, ElementType.Table)

    # TableContext/Table Properties
    NumTables = _TableProperty(False, True, False, None, None, ElementType.TableContext)
    TokenMapper = _TableProperty(False, True, True, None, None, ElementType.TableContext)
    RowCapacityIncr = _TableProperty(False, False, True, "rci", None, ElementType.TableContext, ElementType.Table)
    ColumnCapacityIncr = _TableProperty(False, False, True, "cci", None, ElementType.TableContext, ElementType.Table)
    FreeSpaceThreshold = _TableProperty(False, False, True, "fst", None, ElementType.TableContext, ElementType.Table)
    IsAutoRecalculate = _TableProperty(False, False, True, "recalc", None, ElementType.TableContext, ElementType.Table)
    IsRowLabelsIndexed = _TableProperty(False, False, True, "isRLbX", None, ElementType.TableContext, ElementType.Table)
    IsColumnLabelsIndexed = _TableProperty(
        False, False, True, "isCLbX", None, ElementType.TableContext, ElementType.Table
    )
    IsCellLabelsIndexed = _TableProperty(
        False, False, True, "isClLbX", None, ElementType.TableContext, ElementType.Table
    )
    IsGroupLabelsIndexed = _TableProperty(
        False, False, True, "isGLbX", None, ElementType.TableContext, ElementType.Table
    )
    IsPersistent = _TableProperty(False, False, True, "isP", None, ElementType.TableContext, ElementType.Table)

    # PendingDerivationThreadPool Properties
    IsPendingAllowCoreThreadTimeout = _TableProperty(
        True,
        False,
        True,
        None,
        BaseElementState.PENDINGS_ALLOW_CORE_THREAD_TIMEOUT_FLAG,
        ElementType.TableContext,
        ElementType.Table,
    )
    NumPendingCorePoolThreads = _TableProperty(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    NumPendingMaxPoolThreads = _TableProperty(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    PendingThreadKeepAliveTimeout = _TableProperty(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    PendingThreadKeepAliveTimeoutUnit = _TableProperty(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )
    IsPendingThreadPoolEnabled = _TableProperty(
        True, False, True, None, None, ElementType.TableContext, ElementType.Table
    )

    # Table Element Properties
    NumSubsets = _TableProperty(
        False,
        True,
        False,
        "nSets",
        None,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
    )
    NumRows = _TableProperty(False, True, False, "nRows", None, ElementType.Table, ElementType.Group)
    NumColumns = _TableProperty(False, True, False, "nCols", None, ElementType.Table, ElementType.Group)
    NumCells = _TableProperty(
        False,
        True,
        False,
        "nCells",
        None,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
    )
    NumRowsCapacity = _TableProperty(False, True, False, None, None, ElementType.Table)
    NumColumnsCapacity = _TableProperty(False, True, False, None, None, ElementType.Table)
    NumCellsCapacity = _TableProperty(False, True, False, None, None, ElementType.Column)
    NextCellOffset = _TableProperty(False, True, False, None, None, ElementType.Table)
    Derivation = _TableProperty(
        False,
        False,
        False,
        "fx",
        None,
        ElementType.Column,
        ElementType.Row,
        ElementType.Cell,
    )
    TimeSeries = _TableProperty(False, False, False, "tx", None, ElementType.Column, ElementType.Row)
    Affects = _TableProperty(
        False,
        True,
        False,
        None,
        None,
        ElementType.Table,
        ElementType.Group,
        ElementType.Column,
        ElementType.Row,
        ElementType.Cell,
    )
    Index = _TableProperty(False, True, False, None, None, ElementType.Row, ElementType.Column)
    IsEnforceDataType = _TableProperty(
        False,
        False,
        True,
        "isEDT",
        BaseElementState.ENFORCE_DATATYPE_FLAG,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    IsInUse = _TableProperty(
        False, True, False, None, BaseElementState.IN_USE_FLAG, ElementType.Row, ElementType.Column
    )

    Rows = _TableProperty(False, True, False, None, None, ElementType.Table, ElementType.Group)
    Columns = _TableProperty(False, True, False, None, None, ElementType.Table, ElementType.Group)
    Groups = _TableProperty(
        False,
        True,
        False,
        None,
        None,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
    )
    Cells = _TableProperty(False, True, False, None, None, ElementType.Row, ElementType.Column, ElementType.Group)

    # Cell properties
    Row = _TableProperty(False, True, False, None, None, ElementType.Cell)
    Column = _TableProperty(False, True, False, None, None, ElementType.Cell)
    CellOffset = _TableProperty(False, True, False, None, None, ElementType.Row, ElementType.Cell)
    DataType = _TableProperty(False, False, False, "dt", None, ElementType.Column, ElementType.Cell)

    CellValue = _TableProperty(False, False, False, "v", None, ElementType.Cell)
    ErrorMessage = _TableProperty(True, False, False, "e", None, ElementType.Cell)
    Units = _TableProperty(
        True,
        False,
        False,
        "u",
        None,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    DisplayFormat = _TableProperty(
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
    Validator = _TableProperty(
        True,
        False,
        False,
        "cv",
        None,
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
    def by_name(cls, name: str) -> Property:
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

    @staticmethod
    def by_tag(the_tag: Optional[str] = None) -> Optional[Property]:
        if the_tag is None or the_tag.strip() is None:
            return None
        the_tag = the_tag.strip().lower()
        return _PROPERTIES_BY_TAG[the_tag] if the_tag in _PROPERTIES_BY_TAG else None

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

    def tag(self) -> str:
        if self.value._tag:
            return str(self.value._tag)
        else:
            return self.name

    def is_implemented_by(self, e: Union[ElementType, BaseElement, None]) -> bool:
        from .base_element import BaseElement

        if e is None:
            return False
        if isinstance(e, ElementType):
            return e in self.value._implemented_by
        if isinstance(e, BaseElement):
            return self.is_implemented_by(e.element_type())
        else:
            return False  # type: ignore

    def is_read_only(self) -> bool:
        return bool(self.value._read_only)

    def is_mutable(self) -> bool:
        return not self.value._read_only

    def is_optional(self) -> bool:
        return bool(self.value._optional)

    def is_required(self) -> bool:
        return not self.value._optional

    def is_initializable(self) -> bool:
        return self.value._initializable

    def is_boolean(self) -> bool:
        if self.name.lower().startswith("is"):
            return True

        # handle one-offs by placing them in set
        return self in set([])

    def is_numeric(self) -> bool:
        if self.name.lower().startswith("num"):
            return True

        # handle one-offs by placing them in set
        return self in {
            Property.Index,
            Property.Precision,
            Property.CellOffset,
            Property.NextCellOffset,
            Property.RowCapacityIncr,
            Property.ColumnCapacityIncr,
            Property.FreeSpaceThreshold,
        }

    def is_str(self) -> bool:
        # handle one-offs by placing them in set
        return self in {
            Property.Label,
            Property.Description,
            Property.Units,
            Property.DisplayFormat,
        }


# Define static Tag map
_PROPERTIES_BY_TAG = {p.tag().lower(): p for p in Property}
