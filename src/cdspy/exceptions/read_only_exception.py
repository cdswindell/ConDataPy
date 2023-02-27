from typing import TYPE_CHECKING

from .base_exceptions import TableError

if TYPE_CHECKING:
    from ..elements import BaseElement
    from ..elements import Property


class ReadOnlyException(TableError):
    def __init__(self, be: BaseElement, p: Property) -> None:
        e = be.element_type()
        message = f"ReadOnly: {e.name}->{p.name}"
        super().__init__(e, message)
