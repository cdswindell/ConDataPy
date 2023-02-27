from typing import TYPE_CHECKING

from .base_exceptions import TableError

if TYPE_CHECKING:
    from ..elements import BaseElement
    from ..elements import Property


class UnimplementedException(TableError):
    def __init__(self, be: BaseElement, key: Property) -> None:
        e = be.element_type()
        message = f"Unimplemented: {e.name}->{key.name}"
        super().__init__(e, message)
