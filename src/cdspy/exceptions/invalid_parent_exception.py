from __future__ import annotations

from typing import TYPE_CHECKING

from . import InvalidException

if TYPE_CHECKING:
    from ..elements import BaseElement


class InvalidParentException(InvalidException):
    def __init__(self, parent: BaseElement, child: BaseElement) -> None:
        self._parent = parent
        self._child = child
        message = f"Not child's parent: {parent}->{child}"
        super().__init__(parent.element_type, message)

    @property
    def parent(self) -> BaseElement:
        return self._parent

    @property
    def child(self) -> BaseElement:
        return self._child
