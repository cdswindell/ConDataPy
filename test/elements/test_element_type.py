from __future__ import annotations

from cdspy.elements import ElementType


def test_element_types() -> None:
    assert ElementType.Column.as_reference_label() == "Col"
    assert ElementType.Row.as_reference_label() == "Row"
    assert len(ElementType) == 7
