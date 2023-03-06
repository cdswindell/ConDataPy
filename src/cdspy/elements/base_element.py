from __future__ import annotations

from abc import ABC, abstractmethod
from enum import verify, UNIQUE, Flag
from typing import Any, Final, Optional, Dict, overload, cast, Union

from wrapt import synchronized

from . import ElementType
from . import Property
from ..exceptions import DeletedElementException
from ..exceptions import InvalidPropertyException
from ..exceptions import ReadOnlyException
from ..exceptions import UnimplementedException

TABLE_PROPERTIES_KEY: Final = "_m_props"


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


class BaseElement(ABC):
    """
    BaseElement is the base class for all CDS elements, including Rows, Columns, Cells, and Tables
    """

    @classmethod
    def vet_base_element(cls, be: Optional[BaseElement]) -> None:
        if be is not None and be.is_invalid:
            raise DeletedElementException(be.element_type)

    @property
    @abstractmethod
    def element_type(self) -> ElementType:
        pass

    @property
    @abstractmethod
    def _is_null(self) -> bool:
        pass

    def __init__(self) -> None:
        """
        Constructs a base element, initializing the flags property to NO_FLAGS
        """
        self._m_flags = BaseElementState.NO_FLAGS

    def __getattribute__(self, name: str) -> Any:
        """
        Allow attribute names that match Property keys to return values.
        Note that only properties supported by the BaseElement are returned;

        :param name:
        :return:
        """
        try:
            return super().__getattribute__(name)
        except AttributeError:
            p = Property.by_name("".join(name.title().split("_")), no_raise=True)
            if p and self._implements(p):
                return self.get_property(p)
            else:
                raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        p = Property.by_name("".join(name.title().split("_")), no_raise=True)
        if p:
            # special process for strings
            value = self._normalize(value) if isinstance(value, str) else value
            if value:
                self._set_property(p, value)
            else:
                self._clear_property(p, value)
        else:
            super().__setattr__(name, value)

    def __str__(self) -> str:
        label = self.get_property(Property.Label)
        label = ": " + label if label else ""
        return f"[{self.element_type.name}{label}]"

    def _normalize(self, value: Any) -> Any:
        if isinstance(value, str):
            return " ".join(value.strip().split())
        else:
            return value

    def _implements(self, p: Optional[Property]) -> bool:
        """
        Returns :True: if the :BaseElement: supports
        :Property: :p:
        """
        if p is None:
            return False
        else:
            return p.is_implemented_by(self.element_type)

    def _mutate_flag(self, flag: BaseElementState, state: bool) -> None:
        """Protected method used to modify element flags internal state"""
        if state:
            self._m_flags |= flag
        else:
            self._m_flags &= ~flag

    def _set(self, flag: BaseElementState) -> None:
        self._m_flags |= flag

    def _reset(self, flag: BaseElementState) -> None:
        self._m_flags &= ~flag

    def _is_set(self, flag: BaseElementState) -> bool:
        return (self._m_flags & flag) != BaseElementState.NO_FLAGS

    def _invalidate(self) -> None:
        self._m_flags |= BaseElementState.IS_INVALID_FLAG
        self._reset_element_properties()

    def _vet_element(self, be: Optional[BaseElement] = None) -> None:
        if be is None:
            if self.is_invalid:
                raise DeletedElementException(self.element_type)
        else:
            be._vet_element()

    def __vet_text_key(self, key: Optional[str]) -> str:
        # normalize all string keys
        # replace multiple whitespace with a single space
        key = " ".join(key.strip().lower().split()) if key else None
        if key is None:
            raise InvalidPropertyException(self)
        return key

    @overload
    def _vet_property_key(self, key: Property, for_mutable_op: Optional[bool] = None) -> Property:
        ...

    @overload
    def _vet_property_key(self, key: Optional[str], for_mutable_op: Optional[bool] = None) -> str:
        ...

    def _vet_property_key(
        self, key: Union[Property, str, None], for_mutable_op: Optional[bool] = None
    ) -> Union[Property, str]:
        if key is None:
            raise InvalidPropertyException(self)
        if isinstance(key, Property):
            if not key.is_implemented_by(self.element_type):
                raise UnimplementedException(self, key)
            elif for_mutable_op and key.is_read_only_property:
                raise ReadOnlyException(self, key)
        elif isinstance(key, str):
            key = self.__vet_text_key(key)
        else:
            raise InvalidPropertyException(self, key)
        return key

    @synchronized
    def _reset_element_properties(self) -> None:
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
        key = self._vet_property_key(key, for_mutable_op=True)

        # get the dictionary from the base object, creating it if empty
        properties: dict = cast(dict, self._element_properties(True))

        retval = properties[key] if key in properties else None
        properties[key] = value
        return retval

    @synchronized
    def _clear_property(self, key: Property | str) -> bool:
        key = self._vet_property_key(key, for_mutable_op=True)

        key_present = False
        properties = self._element_properties(False)
        if properties and key in properties:
            properties.pop(key)
            key_present = True
        return key_present

    def has_property(self, key: Property | str) -> bool:
        if isinstance(key, Property):
            if key.is_implemented_by(self.element_type):
                if key.is_required_property:
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

    @overload
    def get_property(self, key: Optional[Property]) -> Any:
        ...

    @overload
    def get_property(self, key: Optional[str]) -> Any:
        ...

    @synchronized
    def get_property(self, key: Union[Property, str, None]) -> Any:
        key = self._vet_property_key(key)

        tprops = self._element_properties()
        if tprops:
            return tprops.get(key, None)
        else:
            return None

    @property
    def is_invalid(self) -> bool:
        """
        Returns True if the element has been deleted, either explicitly or because
        a parent element has been deleted
        :return: True if the element has been deleted
        """
        return self._is_set(BaseElementState.IS_INVALID_FLAG)

    @property
    def is_valid(self) -> bool:
        """
        Returns True if the element has not been deleted
        :return: True if element has not been deleted
        """
        return not self.is_invalid

    @property
    def is_supports_null(self) -> bool:
        return self._is_set(BaseElementState.SUPPORTS_NULL_FLAG)

    @is_supports_null.setter
    def is_supports_null(self, state: bool) -> None:
        self._mutate_flag(BaseElementState.SUPPORTS_NULL_FLAG, state)

    @property
    def is_read_only(self) -> bool:
        return self._is_set(BaseElementState.READONLY_FLAG)

    @is_read_only.setter
    def is_read_only(self, state: bool) -> None:
        self._mutate_flag(BaseElementState.READONLY_FLAG, state)

    @property
    def is_enforce_datatype(self) -> bool:
        return self._is_set(BaseElementState.ENFORCE_DATATYPE_FLAG)

    @is_enforce_datatype.setter
    def is_enforce_datatype(self, state: bool) -> None:
        self._mutate_flag(BaseElementState.ENFORCE_DATATYPE_FLAG, state)
