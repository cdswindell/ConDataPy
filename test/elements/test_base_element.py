from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.elements.base_element import BaseElement


# create test class from BaseElement
class MockBaseElement(BaseElement):
    def __init__(self, et: ElementType) -> None:
        super().__init__()
        self._element_type = et

    def element_type(self) -> ElementType:
        return self._element_type

    def _is_null(self) -> bool:
        return False
