from __future__ import annotations

from typing import TYPE_CHECKING

from .base_exceptions import BaseTableException

if TYPE_CHECKING:
    from ..elements import BaseElement
    from ..elements import Property


class UnimplementedException(BaseTableException):
    def __init__(self, be: BaseElement, key: Property) -> None:
        e = be.element_type()
        message = f"Unimplemented: {e.name}->{key.name}"
        super().__init__(e, message)
