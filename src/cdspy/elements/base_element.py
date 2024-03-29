from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Collection, Sequence

from threading import RLock
from typing import Any, Generic, Iterator, Optional, Dict, overload, cast, TYPE_CHECKING, Tuple, TypeVar, Union
from uuid import UUID

from . import BaseElementState
from . import ElementType
from . import Property
from ..exceptions import DeletedElementException
from ..exceptions import InvalidException
from ..exceptions import InvalidPropertyException
from ..exceptions import ReadOnlyException
from ..exceptions import UnimplementedException
from ..exceptions import UnsupportedException

if TYPE_CHECKING:
    from . import Access
    from . import TableElement


class BaseElement(ABC):
    __slots__: Tuple[str, ...] = ("_state", "_props")
    """
    BaseElement is the base class for all CDS elements, including Rows, Columns, Cells, and Tables
    """

    @staticmethod
    def vet_base_element(be: Optional[BaseElement]) -> None:
        if be and be.is_invalid:
            raise DeletedElementException(be.element_type)

    @staticmethod
    def _parse_quick_action_args(
        qa_map: Dict[str, Access], access: int | Access | None, *args: Any, **kwargs: Any
    ) -> Tuple[int | Access | None, Any]:
        orig_key = list(kwargs.keys())[0]
        key = orig_key.strip().lower()
        if key and key in qa_map:
            access = qa_map[key]
            value = kwargs[orig_key]
            if isinstance(value, str):
                args = tuple([value])
            elif isinstance(value, Collection):
                args = tuple(x for x in value)
            else:
                args = tuple([value])
        return access, args

    @staticmethod
    def _parse_args(arg_type: type, attrib: str, pos: int | None, default: Any, *args: Any, **kwargs: Any) -> Any:
        # named args take priority
        if attrib and attrib in kwargs:
            return kwargs[attrib]
        # then try positional args
        if args and pos is not None and len(args) > pos and args[pos] is not None and isinstance(args[pos], arg_type):
            return args[pos]
        # finally, just return default
        return default

    @staticmethod
    def _normalize(value: Any) -> Any:
        if value:
            if isinstance(value, str):
                s = " ".join(value.strip().split())
                return s if s else None
            elif isinstance(value, tuple) and value[0]:
                value = value[0]
                if isinstance(value, tuple):
                    value = list(value)
        return value

    @staticmethod
    def __normalize_property_value_bool(p: Property, v: object) -> bool:
        from . import TableContext

        if v is None or not isinstance(v, bool):
            v = cast(bool, TableContext().get_property(p))
        return v

    @staticmethod
    def _normalize_uuid(uuid: UUID | str) -> UUID:
        if isinstance(uuid, UUID):
            pass
        elif isinstance(uuid, str):
            uuid = UUID(uuid.strip())
        else:
            raise InvalidException(None, f"'{uuid}' is not a valid GUID/UUID")
        return uuid

    @staticmethod
    def _find_tagged(elems: Sequence[TableElement], *tags: str) -> BaseElement | None:
        from . import Tag

        if elems and tags:
            # we need a context when handling tags
            tc = elems[0].table_context
            # special case single string with comma-separated tags
            if len(tags) == 1 and isinstance(tags[0], str):
                query_tags = Tag.as_tags(tags[0], tc)
            else:
                query_tags = Tag.as_tags(tags, tc)
            if bool(query_tags):
                for te in elems:
                    if te and te.is_valid:
                        te_tags = te._tags
                        if te_tags and te_tags >= query_tags:
                            return te
        return None

    @staticmethod
    def _find(elems: Sequence[TableElement], key: Property | str, *value: object) -> BaseElement | None:
        if not elems:
            return None
        # special case tags
        if key == Property.Tags:
            return BaseElement._find_tagged(elems, *[v for v in value if v and isinstance(v, str)])
        if elems and key and value:
            # value we're looking for is in the first element of the value tuple
            for elem in elems:
                if elem and elem.is_valid:
                    pv = elem.get_property(key)
                    if pv == value[0]:
                        # TODO: set current if row or column
                        return elem
        return None

    @property
    @abstractmethod
    def element_type(self) -> ElementType:
        pass

    @property
    @abstractmethod
    def is_null(self) -> bool:
        pass

    @property
    @abstractmethod
    def lock(self) -> RLock:
        pass

    def __init__(self) -> None:
        """
        Constructs a base element, initializing the flags property to IS_INITIALIZING_FLAG
        """
        self._state = BaseElementState.IS_INITIALIZING_FLAG
        self._props: Dict | None = None

    def __repr__(self) -> str:
        if self.is_invalid:
            return f"[Deleted {self.element_type.name}]"
        else:
            label = ": " + self.label if self.label else ""
            return f"[{self.element_type.name}{label}]"

    def __bool__(self) -> bool:
        """
        Instances of BaseElement should return True even if their length is 0
        :return:
        """
        return True

    def _implements(self, p: Optional[Property]) -> bool:
        """
        Returns :True: if the :BaseElement: supports
        :Property: :p:
        """
        if p is None:
            return False
        else:
            return p.is_implemented_by(self.element_type)

    def _mutate_state(self, state: BaseElementState, value: bool) -> None:
        """Protected method used to modify element flags internal state"""
        if bool(value):
            self._state |= state
        else:
            self._state &= ~state

    def _set(self, state: BaseElementState) -> None:
        self._state |= state

    def _reset(self, state: BaseElementState) -> None:
        self._state &= ~state

    def _is_set(self, state: BaseElementState) -> bool:
        return (self._state & state) != BaseElementState.NO_FLAGS_SET

    def _invalidate(self) -> None:
        self._reset_element_properties()
        self._set(BaseElementState.IS_INVALID_FLAG)

    def vet_element(self, be: Optional[BaseElement] = None, allow_uninitialized: bool = False) -> None:
        if be is None:
            if not self.is_initialized and not bool(allow_uninitialized):
                raise UnsupportedException(
                    self, f"{self.element_type.name} not initialized; did you create via a Table object?"
                )
            if self.is_invalid:
                raise DeletedElementException(self.element_type)
        else:
            be.vet_element()

    @staticmethod
    def vet_elements(*elems: BaseElement) -> None:
        if elems:
            for elem in elems:
                elem.vet_element()

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
            if self.element_type not in key.value._implemented_by:
                raise UnimplementedException(self, key)
            if for_mutable_op and key.is_read_only_property:
                raise ReadOnlyException(self, key)
        elif isinstance(key, str):
            key = self.__vet_text_key(key)
        else:
            raise InvalidPropertyException(self, key)
        return key

    def _reset_element_properties(self) -> None:
        with self.lock:
            if self._props:
                self._props.clear()
                self._props = None

    def _element_properties(self, create_if_empty: bool = False) -> Optional[Dict]:
        with self.lock:
            if self._props is None and create_if_empty:  # type: ignore
                self._props = dict()  # type: ignore
            return self._props

    def _set_property(self, key: Property | str, value: Any) -> Any:
        with self.lock:
            key = self._vet_property_key(key, for_mutable_op=True)

            # get the dictionary from the base object, creating it if empty
            properties: dict = cast(dict, self._element_properties(True))

            retval = properties[key] if key in properties else None
            # for strings , trim leading and trailing white space
            if isinstance(value, str):
                value = value.strip()
            if value is None:
                self._clear_property(key)
            else:
                properties[key] = value
        return retval

    def _initialize_property(self, key: Property | str, value: Any) -> Any:
        with self.lock:
            # vet the key, throw exceptions if invalid
            key = self._vet_property_key(key, for_mutable_op=False)

            # get the dictionary from the base object, creating it if empty
            properties: dict = cast(dict, self._element_properties(True))
            retval = properties.get(key, None)
            if value is None:
                self._clear_property(key)
            else:
                properties[key] = value

            # if this property is a state default, initialize it now
            if isinstance(key, Property) and key.is_state_default_property:
                self._mutate_state(key.state, BaseElement.__normalize_property_value_bool(key, value))
            return retval

    def _clear_property(self, key: Property | str) -> bool:
        with self.lock:
            key = self._vet_property_key(key, for_mutable_op=True)

            key_present = False
            properties = self._element_properties(False)
            if properties and key in properties:
                properties.pop(key)
                key_present = True
            return key_present

    def set_property(self, key: str, value: Any) -> Any:
        # verify the key is a String
        if not isinstance(key, str):
            raise InvalidPropertyException(self, key)
        return self._set_property(key, value)

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

        with self.lock:
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

    def get_property(self, key: Union[Property, str, None]) -> Any:
        with self.lock:
            key = self._vet_property_key(key)
            t_props = self._element_properties()
            if t_props:
                return t_props.get(key, None)
            else:
                return None

    def _delete(self) -> None:
        with self.lock:
            self._reset_element_properties()
            self._invalidate()

    @property
    def is_dirty(self) -> bool:
        return self._is_set(BaseElementState.IS_DIRTY_FLAG)

    def _mark_dirty(self) -> None:
        self._set(BaseElementState.IS_DIRTY_FLAG)

    def _mark_clean(self) -> None:
        self._reset(BaseElementState.IS_DIRTY_FLAG)

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
        self._mutate_state(BaseElementState.SUPPORTS_NULL_FLAG, state)

    @property
    def is_read_only(self) -> bool:
        return self._is_set(BaseElementState.READONLY_FLAG)

    @is_read_only.setter
    def is_read_only(self, state: bool) -> None:
        self._mutate_state(BaseElementState.READONLY_FLAG, state)

    @property
    def is_enforce_datatype(self) -> bool:
        return self._is_set(BaseElementState.ENFORCE_DATATYPE_FLAG)

    @is_enforce_datatype.setter
    def is_enforce_datatype(self, state: bool) -> None:
        self._mutate_state(BaseElementState.ENFORCE_DATATYPE_FLAG, state)

    @property
    def is_initializing(self) -> bool:
        return self._is_set(BaseElementState.IS_INITIALIZING_FLAG)

    @property
    def is_initialized(self) -> bool:
        return not self.is_initializing

    def _mark_initialized(self) -> None:
        self._reset(BaseElementState.IS_INITIALIZING_FLAG)

    @property
    def label(self) -> str | None:
        return cast(str, self.get_property(Property.Label))

    @label.setter
    def label(self, value: Optional[str]) -> None:
        self._set_property(Property.Label, value)

    @property
    def description(self) -> str | None:
        return cast(str, self.get_property(Property.Description))

    @description.setter
    def description(self, value: Optional[str]) -> None:
        self._set_property(Property.Description, value)


_T = TypeVar("_T")


class _BaseElementIterable(Iterator, Generic[_T]):
    __slots__ = ["_elems", "_index"]

    def __init__(self, elems: Collection[_T]) -> None:
        self._elems = tuple(elems)
        self._index = 0

    def __iter__(self) -> Iterator[_T]:
        self._index = 0
        return self

    def __next__(self) -> _T:
        if self._index >= len(self._elems):
            raise StopIteration
        else:
            self._index += 1
            return self._elems.__getitem__(self._index - 1)
