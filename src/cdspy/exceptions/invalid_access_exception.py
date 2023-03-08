from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from . import InvalidException

if TYPE_CHECKING:
    from ..elements import Access
    from ..elements import BaseElement


class InvalidAccessException(InvalidException):
    def __init__(
        self, parent: BaseElement, child: BaseElement, access: Access, is_insert: Optional[bool] = False, *args: object
    ) -> None:
        self._m_parent = parent
        self._m_child = child
        self._m_access = access
        self._m_is_insert = False if is_insert is None else is_insert
        self._m_metadata = args if args else None
        message = f"Invalid {'Insert' if is_insert else 'Get'} Request: {access.name} Child: {child.element_type.name}"
        super().__init__(parent.element_type, message)

    @property
    def parent(self) -> BaseElement:
        return self._m_parent

    @property
    def child(self) -> BaseElement:
        return self._m_child

    @property
    def access(self) -> Access:
        return self._m_access

    @property
    def is_insert(self) -> bool:
        return self._m_is_insert

    @property
    def metadata(self) -> Any:
        return self._m_metadata
