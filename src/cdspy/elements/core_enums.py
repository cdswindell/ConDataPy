"""
ElementType enum defines all components available in the cdsPy
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Union

from enum import Enum, verify, UNIQUE

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
        return sorted({p for p in Property if p.is_implemented_by(self) and p.required_property})

    def optional_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and not p.required_property})

    def initializable_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.initializable_property})

    def read_only_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.read_only_property})

    def mutable_properties(self) -> list[Property]:
        return sorted({p for p in Property if p.is_implemented_by(self) and p.mutable_property})


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
        abbreviation: Optional[str] = None,
        delegates: Optional[list[ElementType]] = None,
        *args: ElementType,
    ) -> None:
        self._optional = optional
        self._read_only = read_only
        self._initializable = initializable
        self._abbreviation = abbreviation
        self._delegates = set(delegates) if delegates else None
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
        None,
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
    def by_abbreviation(abbrev: Optional[str] = None) -> Optional[Property]:
        if abbrev is None or abbrev.strip() is None:
            return None
        abbrev = abbrev.strip().lower()
        return _PROPERTIES_BY_ABBREVIATION[abbrev] if abbrev in _PROPERTIES_BY_ABBREVIATION else None

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
    def tag(self) -> str:
        if self.value._abbreviation:
            return str(self.value._abbreviation)
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

    @property
    def read_only_property(self) -> bool:
        return bool(self.value._read_only)

    @property
    def mutable_property(self) -> bool:
        return not self.value._read_only

    @property
    def optional_property(self) -> bool:
        return bool(self.value._optional)

    @property
    def required_property(self) -> bool:
        return not self.value._optional

    @property
    def initializable_property(self) -> bool:
        return self.value._initializable

    @property
    def boolean_property(self) -> bool:
        if self.name.lower().startswith("is"):
            return True
        # handle one-offs by placing them in set
        return self in set([])

    @property
    def numeric_property(self) -> bool:
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

    @property
    def string_property(self) -> bool:
        # handle one-offs by placing them in set
        return self in {
            Property.Label,
            Property.Description,
            Property.Units,
            Property.DisplayFormat,
        }


# Define static Tag map
_PROPERTIES_BY_ABBREVIATION = {p.tag.lower(): p for p in Property}
