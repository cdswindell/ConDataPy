"""
ElementType enum defines all components available in the cdsPy
"""
from __future__ import annotations

from enum import Enum, verify, UNIQUE


@verify(UNIQUE)
class ElementType(Enum):
    TableContext = 1
    Table = 2
    Row = 3
    Column = 4
    Cell = 5
    Subset = 6
    Derivation = 7

    def as_reference_label(self) -> str:
        if self == ElementType.Column:
            return "Col"
        else:
            return self.name
