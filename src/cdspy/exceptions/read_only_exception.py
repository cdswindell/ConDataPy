from __future__ import annotations

from typing import TYPE_CHECKING

from .base_exceptions import BaseTableException

if TYPE_CHECKING:
    from ..elements import BaseElement
    from ..elements import Property


class ReadOnlyException(BaseTableException):
    def __init__(self, be: BaseElement, p: Property) -> None:
        e = be.element_type()
        message = f"ReadOnly: {e.name}->{p.name}"
        super().__init__(e, message)
