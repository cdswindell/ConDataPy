from __future__ import annotations

from cdspy.elements import ElementType


def test_element_types() -> None:
    assert ElementType.Column.as_reference_label() == "Col"
    assert ElementType.Row.as_reference_label() == "Row"
    assert len(ElementType) == 7
    assert ElementType["TableContext"] == ElementType.TableContext
    assert ElementType["Table"] == ElementType.Table
    assert ElementType["Row"] == ElementType.Row
    assert ElementType["Column"] == ElementType.Column
    assert ElementType["Cell"] == ElementType.Cell
    assert ElementType["Group"] == ElementType.Group
    assert ElementType["Derivation"] == ElementType.Derivation
