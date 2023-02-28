from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.elements import Property
from cdspy.exceptions import UnimplementedException

from ..elements.test_base_element import MockBaseElement


def test_new_unimplemented_exception_with_all_args() -> None:
    me = MockBaseElement(ElementType.Table)
    p = Property.NumRows
    c = UnimplementedException(me, p)
    assert c
    assert type(c) == UnimplementedException
    assert c.element_type() == ElementType.Table
    assert c.message() == f"Unimplemented: {me.element_type().name}->{p.name}"
