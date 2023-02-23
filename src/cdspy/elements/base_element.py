from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from enum import Flag, verify, UNIQUE


@verify(UNIQUE)
class CF(Flag):
    NO_FLAGS = 0x0

    ENFORCE_DATATYPE_FLAG = 0x01
    READONLY_FLAG = 0x02
    SUPPORTS_NULL_FLAG = 0x04
    AUTO_RECALCULATE_FLAG = 0x08

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

    def __mutate_flag(
        self, set_values: Optional[CF] = None, unset_values: Optional[CF] = None
    ) -> None:
        """Protected method used to modify element flags internal state"""
        if set_values:
            self.m_flags |= set_values
        elif unset_values:
            self.m_flags &= ~unset_values

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
