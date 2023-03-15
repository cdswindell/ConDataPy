from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from .base_exceptions import BaseTableException

if TYPE_CHECKING:
    from ..elements import BaseElement
    from ..elements import ElementType


class InvalidException(BaseTableException):
    def __init__(self, et: Optional[ElementType | BaseElement] = None, message: Optional[str] = None) -> None:
        super().__init__(et, message)
