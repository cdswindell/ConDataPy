from __future__ import annotations

from typing import Any, TYPE_CHECKING

from . import InvalidException

if TYPE_CHECKING:
    from ..elements import Access
    from ..elements import ElementType
    from ..elements import BaseElement


class InvalidAccessException(InvalidException):
    def __init__(
        self,
        parent: BaseElement,
        child: BaseElement | ElementType,
        access: Access,
        is_insert: bool = False,
        *args: object,
    ) -> None:
        from ..elements import BaseElement

        self._parent = parent
        self._child = child if isinstance(child, BaseElement) else None
        self._child_type = child.element_type if isinstance(child, BaseElement) else child
        self._access = access
        self._is_insert = False if is_insert is None else bool(is_insert)
        self._metadata = args if args else None
        message = f"Invalid {'Insert' if is_insert else 'Get'} Request: {access.name} Child: {self._child_type.name}"
        super().__init__(parent.element_type, message)

    @property
    def parent(self) -> BaseElement:
        return self._parent

    @property
    def child(self) -> BaseElement | None:
        return self._child

    @property
    def child_type(self) -> ElementType:
        return self._child_type

    @property
    def access(self) -> Access:
        return self._access

    @property
    def is_insert(self) -> bool:
        return self._is_insert

    @property
    def metadata(self) -> Any:
        return self._metadata
