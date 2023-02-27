from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from enum import Flag, verify, UNIQUE

from .element_type import ElementType
from ..exceptions import DeletedElementException


@verify(UNIQUE)
class CF(Flag):
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


class BaseElement(ABC):
    """
    BaseElement is the base class for all CDS elements, including Rows, Columns, Cells, and Tables
    """

    def __init__(self) -> None:
        self.m_flags = CF.NO_FLAGS

    @abstractmethod
    def is_null(self) -> bool:
        pass

    @abstractmethod
    def reset_elem_properties(self) -> None:
        pass

    @abstractmethod
    def element_type(self) -> ElementType:
        pass

    @abstractmethod
    def _element_properties(self, create_if_empty: bool = False) -> dict:
        pass

    @classmethod
    def vet_base_element(cls, be: Optional[BaseElement]) -> None:
        if be is not None and be.is_invalid():
            raise DeletedElementException(be.element_type())

    def __mutate_flag(
        self, set_values: Optional[CF] = None, unset_values: Optional[CF] = None
    ) -> None:
        """Protected method used to modify element flags internal state"""
        if set_values:
            self.m_flags |= set_values
        elif unset_values:
            self.m_flags &= ~unset_values

    def vet_element(self, be: Optional[BaseElement] = None) -> None:
        if be is None:
            if self.is_invalid():
                raise DeletedElementException(self.element_type())
        else:
            be.vet_element()

    def is_set(self, flag: CF) -> bool:
        return (self.m_flags & flag) != CF.NO_FLAGS

    def unset(self, flag: CF) -> None:
        self.__mutate_flag(unset_values=flag)

    def invalidate(self) -> None:
        self.m_flags |= CF.IS_INVALID_FLAG
        self.reset_elem_properties()

    def is_invalid(self) -> bool:
        """
        Returns True if the element has been deleted, either explicitly or because
        a parent element has been deleted
        :return: True if the element has been deleted
        """
        return self.is_set(CF.IS_INVALID_FLAG)

    def is_valid(self) -> bool:
        """
        Returns True if the element has not been deleted
        :return: True if the element has not been deleted
        """
        return not self.is_invalid()
