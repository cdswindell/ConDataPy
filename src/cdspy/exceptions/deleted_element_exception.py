from typing import Optional

from ..elements import ElementType
from .base_exceptions import TableError


class DeletedElementException(TableError):
    def __init__(
        self, e: Optional[ElementType] = None, message: Optional[str] = None
    ) -> None:
        if message is None:
            message = f"Operations on deleted {e.name}s are not allowed"
        super().__init__(message)
