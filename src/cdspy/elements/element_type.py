"""
ElementType enum defines all components available in the cdsPy
"""
from __future__ import annotations

from enum import Enum, verify, UNIQUE


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
