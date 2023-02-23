from __future__ import annotations
from typing import Optional

from enum import Enum, verify, UNIQUE

from .element_type import ElementType


class _TableProperty:
    def __init__(
        self,
        _optional: bool,
        read_only: bool,
        initializable: bool,
        tag: Optional[str],
        *args: ElementType,
    ) -> None:
        self.optional = _optional
        self.read_only = read_only
        self.initializable = initializable
        self.tag = tag
        self.implemented_by = set(args) if args else set(ElementType)


@verify(UNIQUE)
class Property(Enum):
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

    def is_read_only(self) -> bool:
        return bool(self.value.read_only)

    def is_mutable(self) -> bool:
        return not self.value.read_only

    def is_optional(self) -> bool:
        return bool(self.value.optional)

    def is_required(self) -> bool:
        return not self.value.optional

    def tag(self) -> str:
        if self.value.tag:
            return str(self.value.tag)
        else:
            return self.name

    def is_boolean(self) -> bool:
        if self.name.lower().startswith("is"):
            return True

        # handle one-offs by placing them in set
        return self in set([])

    def is_str(self) -> bool:
        # handle one-offs by placing them in set
        return self in set(
            [
                Property.Label,
                Property.Description,
                Property.Units,
                Property.DisplayFormat,
            ]
        )
