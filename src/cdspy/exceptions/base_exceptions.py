from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..elements import BaseElement
    from ..elements import ElementType


class BaseTableException(RuntimeError):
    def __init__(self, et: Optional[ElementType | BaseElement] = None, message: Optional[str] = None) -> None:
        from ..elements import BaseElement

        super().__init__(message)
        self._element_type = et.element_type if isinstance(et, BaseElement) else et
        self._message = message

    @property
    def element_type(self) -> Optional[ElementType]:
        return self._element_type

    @property
    def message(self) -> Optional[str]:
        return self._message
