"""
ElementType enum defines all components available in the cdsPy
"""
from __future__ import annotations
from typing import TYPE_CHECKING

from enum import Enum, verify, UNIQUE

if TYPE_CHECKING:
    from . import Property


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
        # to prevent circular imports, we must import Properties within the function
        from . import Property

        return sorted({p for p in Property if p.is_implemented_by(self)})

    def required_properties(self) -> list[Property]:
        from . import Property

        return sorted(
            {p for p in Property if p.is_implemented_by(self) and p.is_required()}
        )

    def optional_properties(self) -> list[Property]:
        from . import Property

        return sorted(
            {p for p in Property if p.is_implemented_by(self) and not p.is_required()}
        )

    def initializable_properties(self) -> list[Property]:
        from . import Property

        return sorted(
            {p for p in Property if p.is_implemented_by(self) and p.is_initializable()}
        )

    def read_only_properties(self) -> list[Property]:
        from . import Property

        return sorted(
            {p for p in Property if p.is_implemented_by(self) and p.is_read_only()}
        )

    def mutable_properties(self) -> list[Property]:
        from . import Property

        return sorted(
            {p for p in Property if p.is_implemented_by(self) and p.is_mutable()}
        )
