from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from . import InvalidException

if TYPE_CHECKING:
    from ..elements import BaseElement


class InvalidParentException(InvalidException):
    def __init__(self, parent: Optional[BaseElement], child: Optional[BaseElement]) -> None:
        self._parent = parent
        self._child = child
        if parent is None and child is not None:  # type: ignore[union-attr]
            message = f"Parentless child: {child}"
        elif parent is not None and child is None:
            message = f"Childless Parent: {parent}"
        else:
            message = f"Not child's parent: {parent}->{child}"
        super().__init__(parent.element_type if parent else None, message)

    @property
    def parent(self) -> BaseElement | None:
        return self._parent

    @property
    def child(self) -> BaseElement | None:
        return self._child
