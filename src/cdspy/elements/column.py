from __future__ import annotations

from typing import Collection, Optional

from . import ElementType
from . import TableElement
from . import TableSliceElement
from ..mixins import Derivable


class Column(TableSliceElement):
    def __init__(self, te: TableElement) -> None:
        super().__init__(te)

    @property
    def element_type(self) -> ElementType:
        return ElementType.Column

    def fill(self, o: Optional[object]) -> None:
        pass

    @property
    def num_cells(self) -> int:
        pass

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table.is_column_labels_indexed) if self.table else False

    @property
    def derived_elements(self) -> Collection[Derivable]:
        pass

    @property
    def is_null(self) -> bool:
        pass
