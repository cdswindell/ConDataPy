from typing import Optional, TYPE_CHECKING, Union

from .base_exceptions import TableError

if TYPE_CHECKING:
    from ..elements import BaseElement
    from ..elements import Property


class InvalidPropertyException(TableError):
    def __init__(
        self,
        be: BaseElement,
        key: Union[Property, str, None] = None,
        message: Optional[str] = None,
    ) -> None:
        e = be.element_type()

        if message is None:
            message = "Property not specified"

        if isinstance(key, str):
            key = key.strip() if key and key.strip() else "<not specified>"
            message = f"Invalid property: '{key}'"
        elif isinstance(key, Property):
            message = f"Invalid: {e.name}->{key.name}"
        super().__init__(e, message)
