from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from .base_exceptions import BaseTableException

if TYPE_CHECKING:
    from ..elements import ElementType


class InvalidException(BaseTableException):
    def __init__(self, et: Optional[ElementType] = None, message: Optional[str] = None) -> None:
        super().__init__(et, message)
