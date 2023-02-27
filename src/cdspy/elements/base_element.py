from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Final, Optional

from enum import Flag, verify, UNIQUE

from .element_type import ElementType
from .table_property import Property
from ..exceptions import DeletedElementException
from ..exceptions import InvalidPropertyException
from ..exceptions import ReadOnlyException
from ..exceptions import UnimplementedException


@verify(UNIQUE)
class BaseElementState(Flag):
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


# Used to decorate string property keys
RESERVED_PROPERTY_PREFIX: Final = "~~~"


class BaseElement(ABC):
    """
    BaseElement is the base class for all CDS elements, including Rows, Columns, Cells, and Tables
    """

    @classmethod
    def vet_base_element(cls, be: Optional[BaseElement]) -> None:
        if be is not None and be.is_invalid():
            raise DeletedElementException(be.element_type())

    @abstractmethod
    def element_type(self) -> ElementType:
        pass

    @abstractmethod
    def _is_null(self) -> bool:
        pass

    @abstractmethod
    def _reset_elem_properties(self) -> None:
        pass

    @abstractmethod
    def _element_properties(self, create_if_empty: bool = False) -> dict:
        pass

    def __init__(self) -> None:
        """
        Constructs a base element, initializing the flags property to empty
        """
        self._m_flags = BaseElementState.NO_FLAGS

    def _implements(self, p: Optional[Property]) -> bool:
        if p is None:
            return False
        else:
            return p.is_implemented_by(self.element_type())

    def __mutate_flag(
        self,
        set_values: Optional[BaseElementState] = None,
        unset_values: Optional[BaseElementState] = None,
    ) -> None:
        """Protected method used to modify element flags internal state"""
        if set_values:
            self._m_flags |= set_values
        elif unset_values:
            self._m_flags &= ~unset_values

    def __vet_key(self, key: Optional[str]) -> str:
        if key is None or not key.strip():
            raise InvalidPropertyException(self)
        return key

    def _unset(self, flag: BaseElementState) -> None:
        self.__mutate_flag(unset_values=flag)

    def _is_set(self, flag: BaseElementState) -> bool:
        return (self._m_flags & flag) != BaseElementState.NO_FLAGS

    def _invalidate(self) -> None:
        self._m_flags |= BaseElementState.IS_INVALID_FLAG
        self._reset_elem_properties()

    def _set_property(self, key: Property | str, value: Any) -> Any:
        if isinstance(key, Property):
            if not key.is_implemented_by(self.element_type()):
                raise UnimplementedException(self, key)
            elif key.is_read_only():
                raise ReadOnlyException(self, key)
        elif isinstance(key, str):
            key = key.strip()
            if not key:
                raise AttributeError("Property keys can not be None or Empty")

        else:
            raise AttributeError(f"Unsupported property key type: {type(key)}")

        # get the dictionary from the base object, creating it if empty
        properties = self._element_properties(True)

        retval = properties[key] if key in properties else None
        properties[key] = value
        return retval

    def is_invalid(self) -> bool:
        """
        Returns True if the element has been deleted, either explicitly or because
        a parent element has been deleted
        :return: True if the element has been deleted
        """
        return self._is_set(BaseElementState.IS_INVALID_FLAG)

    def is_valid(self) -> bool:
        """
        Returns True if the element has not been deleted
        :return: True if element has not been deleted
        """
        return not self.is_invalid()

    def vet_element(self, be: Optional[BaseElement] = None) -> None:
        if be is None:
            if self.is_invalid():
                raise DeletedElementException(self.element_type())
        else:
            be.vet_element()
