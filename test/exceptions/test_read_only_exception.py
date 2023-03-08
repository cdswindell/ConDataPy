from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.elements import Property
from cdspy.exceptions import ReadOnlyException

from ..elements.test_base_element import MockBaseElement


def test_new_read_only_exception_with_all_args() -> None:
    me = MockBaseElement(ElementType.Table)
    p = Property.Tags
    c = ReadOnlyException(me, p)
    assert c
    assert type(c) == ReadOnlyException
    assert c.element_type == ElementType.Table
    assert c.message == f"ReadOnly: {me.element_type.name}->{p.name}"
