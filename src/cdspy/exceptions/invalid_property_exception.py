from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Union

from ..elements import Property
from .base_exceptions import BaseTableException

if TYPE_CHECKING:
    from ..elements.base_element import BaseElement


class InvalidPropertyException(BaseTableException):
    def __init__(
        self,
        be: BaseElement,
        key: Union[Property, str, None] = None,
        message: Optional[str] = None,
    ) -> None:
        e = be.element_type

        if message is None:
            message = "Property not specified"

        if isinstance(key, str):
            key = key.strip() if key and key.strip() else "<not specified>"
            message = f"Invalid: {e.name}->'{key}'"
        elif isinstance(key, Property):
            message = f"Invalid: {e.name}->{key.name}"
        elif key:
            message = f"Invalid Property: {type(key)}"  # type: ignore
        super().__init__(e, message)
