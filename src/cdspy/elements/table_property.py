from __future__ import annotations

from typing import Optional, Any

from enum import Enum, verify, UNIQUE

from . import ElementType


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
        tag: Optional[str],
        *args: ElementType,
    ) -> None:
        self._optional = optional
        self._read_only = read_only
        self._initializable = initializable
        self._tag = tag
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
    IsNull = _TableProperty(
        False,
        True,
        False,
        None,
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
        ElementType.Group,
    )
    IsSupportsNull = _TableProperty(
        False,
        False,
        True,
        "isNulls",
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
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
        ElementType.Group,
    )
    Precision = _TableProperty(
        True, False, True, "pr", ElementType.TableContext, ElementType.Table
    )

    # TableContext/Table Properties
    NumTables = _TableProperty(False, True, False, None, ElementType.TableContext)
    TokenMapper = _TableProperty(False, True, True, None, ElementType.TableContext)
    RowCapacityIncr = _TableProperty(
        False, False, True, "rci", ElementType.TableContext, ElementType.Table
    )
    ColumnCapacityIncr = _TableProperty(
        False, False, True, "cci", ElementType.TableContext, ElementType.Table
    )
    FreeSpaceThreshold = _TableProperty(
        False, False, True, "fst", ElementType.TableContext, ElementType.Table
    )
    IsAutoRecalculate = _TableProperty(
        False, False, True, "recalc", ElementType.TableContext, ElementType.Table
    )
    IsRowLabelsIndexed = _TableProperty(
        False, False, True, "isRLbX", ElementType.TableContext, ElementType.Table
    )
    IsColumnLabelsIndexed = _TableProperty(
        False, False, True, "isCLbX", ElementType.TableContext, ElementType.Table
    )
    IsCellLabelsIndexed = _TableProperty(
        False, False, True, "isClLbX", ElementType.TableContext, ElementType.Table
    )
    IsGroupLabelsIndexed = _TableProperty(
        False, False, True, "isGLbX", ElementType.TableContext, ElementType.Table
    )
    IsPersistent = _TableProperty(
        False, False, True, "isP", ElementType.TableContext, ElementType.Table
    )

    # PendingDerivationThreadPool Properties
    IsPendingAllowCoreThreadTimeout = _TableProperty(
        True, False, True, None, ElementType.TableContext, ElementType.Table
    )
    NumPendingCorePoolThreads = _TableProperty(
        True, False, True, None, ElementType.TableContext, ElementType.Table
    )
    NumPendingMaxPoolThreads = _TableProperty(
        True, False, True, None, ElementType.TableContext, ElementType.Table
    )
    PendingThreadKeepAliveTimeout = _TableProperty(
        True, False, True, None, ElementType.TableContext, ElementType.Table
    )
    PendingThreadKeepAliveTimeoutUnit = _TableProperty(
        True, False, True, None, ElementType.TableContext, ElementType.Table
    )
    IsPendingThreadPoolEnabled = _TableProperty(
        True, False, True, None, ElementType.TableContext, ElementType.Table
    )

    # Table Element Properties
    NumSubsets = _TableProperty(
        False,
        True,
        False,
        "nSets",
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
    )
    NumRows = _TableProperty(
        False, True, False, "nRows", ElementType.Table, ElementType.Group
    )
    NumColumns = _TableProperty(
        False, True, False, "nCols", ElementType.Table, ElementType.Group
    )
    NumCells = _TableProperty(
        False,
        True,
        False,
        "nCells",
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
    )
    NumRowsCapacity = _TableProperty(False, True, False, None, ElementType.Table)
    NumColumnsCapacity = _TableProperty(False, True, False, None, ElementType.Table)
    NumCellsCapacity = _TableProperty(False, True, False, None, ElementType.Column)
    NextCellOffset = _TableProperty(False, True, False, None, ElementType.Table)
    Derivation = _TableProperty(
        False,
        False,
        False,
        "fx",
        ElementType.Column,
        ElementType.Row,
        ElementType.Cell,
    )
    TimeSeries = _TableProperty(
        False, False, False, "tx", ElementType.Column, ElementType.Row
    )
    Affects = _TableProperty(
        False,
        True,
        False,
        None,
        ElementType.Table,
        ElementType.Group,
        ElementType.Column,
        ElementType.Row,
        ElementType.Cell,
    )
    Index = _TableProperty(
        False, True, False, None, ElementType.Row, ElementType.Column
    )
    IsEnforceDataType = _TableProperty(
        False,
        False,
        True,
        "isEDT",
        ElementType.TableContext,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    IsInUse = _TableProperty(
        False, True, False, None, ElementType.Row, ElementType.Column
    )

    Rows = _TableProperty(
        False, True, False, None, ElementType.Table, ElementType.Group
    )
    Columns = _TableProperty(
        False, True, False, None, ElementType.Table, ElementType.Group
    )
    Groups = _TableProperty(
        False,
        True,
        False,
        None,
        ElementType.Table,
        ElementType.Row,
        ElementType.Column,
        ElementType.Group,
    )
    Cells = _TableProperty(
        False, True, False, None, ElementType.Row, ElementType.Column, ElementType.Group
    )

    # Cell properties
    Row = _TableProperty(False, True, False, None, ElementType.Cell)
    Column = _TableProperty(False, True, False, None, ElementType.Cell)
    CellOffset = _TableProperty(
        False, True, False, None, ElementType.Row, ElementType.Cell
    )
    DataType = _TableProperty(
        False, False, False, "dt", ElementType.Column, ElementType.Cell
    )

    CellValue = _TableProperty(False, False, False, "v", ElementType.Cell)
    ErrorMessage = _TableProperty(True, False, False, "e", ElementType.Cell)
    Units = _TableProperty(
        True,
        False,
        False,
        "u",
        ElementType.Row,
        ElementType.Column,
        ElementType.Cell,
    )
    DisplayFormat = _TableProperty(
        True,
        False,
        True,
        "f",
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
        if name in cls.__members__:
            return cls[name]
        # fall back to case-insensitive s
        name = name.lower()
        for k, v in cls.__members__.items():
            if k.lower() == name:
                return v
        else:
            raise ValueError(f"'{name}' is not a valid {cls.__name__}")

    @staticmethod
    def by_tag(tag: str) -> Optional[Property]:
        if not tag and not tag.strip():
            return None
        tag = tag.strip().lower()
        return _PROPERTIES_BY_TAG[tag] if tag in _PROPERTIES_BY_TAG else None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Property):
            return False
        return self.name == other.name

    def __lt__(self, other: Property) -> bool:
        if not isinstance(other, Property):
            raise NotImplementedError
        return self.name < other.name

    def __hash__(self) -> int:
        return self.name.__hash__()

    def tag(self) -> str:
        if self.value._tag:
            return str(self.value._tag)
        else:
            return self.name

    def is_implemented_by(self, e: Any) -> bool:
        if e is None:
            return False
        if isinstance(e, ElementType):
            return e in self.value._implemented_by
        if callable(getattr(e, "element_type", None)):
            return self.is_implemented_by(e.element_type())
        return e in self.value._implemented_by

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
