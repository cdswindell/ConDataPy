from typing import Optional

from ..elements import ElementType


class TableError(RuntimeError):
    def __init__(
        self, et: Optional[ElementType] = None, message: Optional[str] = None
    ) -> None:
        super().__init__(message)
        self.element_type = et
        self.message = message
