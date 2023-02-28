from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..elements import ElementType


class BaseTableException(RuntimeError):
    def __init__(
        self, et: Optional[ElementType] = None, message: Optional[str] = None
    ) -> None:
        super().__init__(message)
        self._element_type = et
        self._message = message

    def element_type(self) -> Optional[ElementType]:
        return self._element_type

    def message(self) -> Optional[str]:
        return self._message
