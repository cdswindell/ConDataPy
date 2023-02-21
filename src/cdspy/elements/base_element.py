from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

SF_ENFORCE_DATATYPE_FLAG = 0x01
SF_READONLY_FLAG = 0x02
SF_SUPPORTS_NULL_FLAG = 0x04
SF_AUTO_RECALCULATE_FLAG = 0x08

SF_IS_INVALID_FLAG = 0x10000000
SF_IS_PROCESSED_FLAG = 0x20000000


class BaseElement(ABC):
    """
    BaseElement is the base class for all CDS elements, including Rows, Columns, Cells, and Tables
    """

    def __init__(self) -> None:
        self.m_flags = 0x0

    @abstractmethod
    def is_null(self) -> bool:
        pass

    @abstractmethod
    def reset_elem_properties(self) -> None:
        pass

    def __mutate_flag(
        self, set_values: Optional[int] = None, unset_values: Optional[int] = None
    ) -> None:
        """Protected method used to modify element flags internal state"""
        if set_values:
            self.m_flags |= set_values
        elif unset_values:
            self.m_flags &= ~unset_values

    def is_set(self, flag: int) -> bool:
        return (self.m_flags & flag) != 0

    def unset(self, flag: int) -> None:
        self.__mutate_flag(unset_values=flag)

    def invalidate(self) -> None:
        self.m_flags |= SF_IS_INVALID_FLAG
        self.reset_elem_properties()

    def is_invalid(self) -> bool:
        """
        Returns True if the element has been deleted, either explicitly or because
        a parent element has been deleted
        :return: True if the element has been deleted
        """
        return self.is_set(SF_IS_INVALID_FLAG)

    def is_valid(self) -> bool:
        """
        Returns True if the element has not been deleted
        :return: True if the element has not been deleted
        """
        return not self.is_invalid()
