from __future__ import annotations

from typing import Collection, Optional, TYPE_CHECKING

from . import ElementType
from . import TableElement
from . import TableSliceElement
from ..mixins import Derivable

if TYPE_CHECKING:
    from . import Column
    from . import Cell


class Row(TableSliceElement):
    def __init__(self, te: TableElement) -> None:
        super().__init__(te)
        self.__cell_offset = -1

        # initialize properties
        for p in self.element_type.initializable_properties():
            value = self._get_template(te).get_property(p)
            self._initialize_property(p, value)
        self._mark_initialized()

    def _delete(self, compress: Optional[bool] = True) -> None:
        try:
            super()._delete(compress)
        except KeyError:
            # TODO: event handler stuff
            pass

        self._set_cell_offset(-1)
        self._set_index(-1)
        self._set_is_in_use(False)
        self.invalidate()

    @property
    def _cell_offset(self) -> int:
        return self.__cell_offset

    def _set_cell_offset(self, offset: int) -> None:
        self.__cell_offset = offset
        if offset > 0 and self.table:
            self.table._map_cell_offset_to_row(self, offset)

    def _get_cell(
        self, col: Column, set_to_current: Optional[bool] = True, create_if_sparse: Optional[bool] = True
    ) -> Cell:
        self.vet_components(col)
        return col._cell(col, set_to_current=True, create_if_sparse=True)

    def get_cell(self, col: Column) -> Cell:
        return self._get_cell(col, set_to_current=True, create_if_sparse=True)

    @property
    def element_type(self) -> ElementType:
        return ElementType.Row

    @property
    def num_slices(self) -> int:
        return self.table.num_columns if self.table else 0

    @property
    def slices_type(self) -> ElementType:
        return ElementType.Column

    def mark_current(self) -> Row:
        self.vet_element()
        return self.table.mark_current(self) if self.table else None

    @property
    def num_cells(self) -> int:
        return 0

    @property
    def is_null(self) -> bool:
        return self.num_cells == 0

    @property
    def is_label_indexed(self) -> bool:
        return bool(self.table.is_row_labels_indexed) if self.table else False

    @property
    def derived_elements(self) -> Collection[Derivable]:
        pass
