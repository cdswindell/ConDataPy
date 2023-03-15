from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from .base_exceptions import BaseTableException

if TYPE_CHECKING:
    from ..elements import ElementType
    from ..elements import BaseElement


class UnsupportedException(BaseTableException):
    def __init__(self, be: BaseElement | ElementType, message: Optional[str] = None) -> None:
        from ..elements import BaseElement

        e = be.element_type if isinstance(be, BaseElement) else be
        message = message.strip() if message and message.strip() else f"Unsupported on {e.name}"
        super().__init__(e, message)
