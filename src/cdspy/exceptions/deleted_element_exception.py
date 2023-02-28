from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from .base_exceptions import BaseTableException

if TYPE_CHECKING:
    from ..elements import ElementType


class DeletedElementException(BaseTableException):
    def __init__(
        self, e: Optional[ElementType] = None, message: Optional[str] = None
    ) -> None:
        if message is None:
            if e is None:
                message = "Operations on deleted elements are not allowed"
            else:
                message = f"Operations on deleted {e.name}s are not allowed"
        super().__init__(e, message)
