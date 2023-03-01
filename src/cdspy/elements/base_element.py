from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Final, Optional, Dict, overload, cast, Union

from wrapt import synchronized

from . import ElementType
from . import BaseElementState
from . import Property
from ..exceptions import DeletedElementException
from ..exceptions import InvalidPropertyException
from ..exceptions import ReadOnlyException
from ..exceptions import UnimplementedException

TABLE_PROPERTIES_KEY: Final = "_m_tprops"


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

    @overload
    def __vet_key_for_mutable_op(self, key: Property) -> Property:
        ...

    @overload
    def __vet_key_for_mutable_op(self, key: Optional[str]) -> str:
        ...

    def __vet_key_for_mutable_op(self, key: Union[Property, str, None]) -> Property | str:
        if key is None:
            raise InvalidPropertyException(self)
        elif isinstance(key, Property):
            if not key.is_implemented_by(self.element_type()):
                raise UnimplementedException(self, key)
            elif key.is_read_only():
                raise ReadOnlyException(self, key)
        elif isinstance(key, str):
            key = self.__vet_text_key(key)
        else:
            raise AttributeError(f"Unsupported property key type: {type(key)}")
        return key

    def __vet_text_key(self, key: Optional[str]) -> str:
        # normalize all string keys
        # replace multiple whitespace with a single space
        key = " ".join(key.strip().lower().split()) if key else None
        if key is None:
            raise InvalidPropertyException(self)
        return key

    def _unset(self, flag: BaseElementState) -> None:
        self.__mutate_flag(unset_values=flag)

    def _is_set(self, flag: BaseElementState) -> bool:
        return (self._m_flags & flag) != BaseElementState.NO_FLAGS

    def _invalidate(self) -> None:
        self._m_flags |= BaseElementState.IS_INVALID_FLAG
        self._reset_elem_properties()

    @synchronized
    def _reset_elem_properties(self) -> None:
        t_props: Dict = vars(self).pop(TABLE_PROPERTIES_KEY, None)
        if t_props:
            t_props.clear()

    @synchronized
    def _element_properties(self, create_if_empty: bool = False) -> Optional[Dict]:
        t_props: Dict = vars(self).get(TABLE_PROPERTIES_KEY, None)
        if t_props is None and create_if_empty:  # type: ignore
            t_props = dict()  # type: ignore
            setattr(self, TABLE_PROPERTIES_KEY, t_props)
        return t_props

    @synchronized
    def _set_property(self, key: Property | str, value: Any) -> Any:
        key = self.__vet_key_for_mutable_op(key)

        # get the dictionary from the base object, creating it if empty
        properties: dict = cast(dict, self._element_properties(True))

        retval = properties[key] if key in properties else None
        properties[key] = value
        return retval

    @synchronized
    def _clear_property(self, key: Property | str) -> bool:
        key = self.__vet_key_for_mutable_op(key)

        key_present = False
        properties = self._element_properties(False)
        if properties and key in properties:
            properties.pop(key)
            key_present = True
        return key_present

    def has_property(self, key: Property | str) -> bool:
        if isinstance(key, Property):
            if key.is_implemented_by(self.element_type()):
                if key.is_required():
                    # required properties are always present, even if not yet set!
                    return True
                else:
                    pass  # check if key is defined in the dictionary
            else:
                return False  # table element doesn't support key
        elif isinstance(key, str):
            key = self.__vet_text_key(key)

        with synchronized(self):
            properties = self._element_properties(False)
            if properties and key in properties:
                return True
        return False

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
