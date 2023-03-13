from __future__ import annotations

from abc import ABC
from typing import Optional, Set
from uuid import UUID

from ..utils import JustInTimeSet

from ..exceptions import InvalidException

from .base_element import _BaseElementIterable
from . import BaseElementState
from . import TableElement
from . import TableCellsElement


class TableSliceElement(TableCellsElement, ABC):
    def __init__(self, te: Optional[TableElement] = None) -> None:
        super().__init__(te)
        self._remote_uuids: Set[UUID] = set()
        self._groups = JustInTimeSet()

    @property
    def is_in_use(self) -> bool:
        return self._is_set(BaseElementState.IN_USE_FLAG)

    def _set_is_in_use(self, value: bool) -> None:
        self._mutate_state(BaseElementState.IN_USE_FLAG, value)

    def register_remote_uuid(self, uuid: UUID | str) -> None:
        if uuid:
            self._remote_uuids.add(self._normalize_uuid(uuid))

    def deregister_remote_uuid(self, uuid: UUID | str) -> None:
        if uuid:
            self._remote_uuids.discard(self._normalize_uuid(uuid))

    @property
    def is_datatype_enforced(self) -> bool:
        """
        Tables, rows, columns, and cells each can specify if data typing is
        enforced via the is_enforce_datatype property. This property helps to
        walk the object hierarchy backwards from most-general (table) to least-
        general TableElement

        :return: True if data typing is enforced for this object
        """
        if self.is_enforce_datatype:
            return True
        return self.table.is_datatype_enforced if self.table else False

    @property
    def is_nulls_supported(self) -> bool:
        if self.is_supports_null:
            return True
        return self.table.is_nulls_supported if self.table else False

    @property
    def num_groups(self) -> int:
        return len(self._groups)
