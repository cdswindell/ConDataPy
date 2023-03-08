from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.elements import Access
from cdspy.exceptions import InvalidAccessException

from ..elements.test_base_element import MockBaseElement


def test_new_invalid_access_exception() -> None:
    p = MockBaseElement(ElementType.Table)
    c = MockBaseElement(ElementType.Row)
    e = InvalidAccessException(p, c, Access.Current)
    assert e
    assert type(e) == InvalidAccessException
    assert e.element_type == ElementType.Table
    assert e.message == "Invalid Get Request: Current Child: Row"
    assert not e.is_insert
    assert e.parent == p
    assert e.child == c
    assert e.access == Access.Current
    assert e.metadata is None

    e = InvalidAccessException(p, c, Access.Next, True)
    assert e
    assert type(e) == InvalidAccessException
    assert e.element_type == ElementType.Table
    assert e.message == "Invalid Insert Request: Next Child: Row"
    assert e.is_insert
    assert e.child == c
    assert e.access == Access.Next
    assert e.metadata is None

    e = InvalidAccessException(p, c, Access.ByIndex, False, 1)
    assert e
    assert type(e) == InvalidAccessException
    assert e.element_type == ElementType.Table
    assert e.message == "Invalid Get Request: ByIndex Child: Row"
    assert not e.is_insert
    assert e.child == c
    assert e.access == Access.ByIndex
    assert e.metadata == (1,)

    e = InvalidAccessException(p, c, Access.ByLabel, False, "abc")
    assert e
    assert type(e) == InvalidAccessException
    assert e.element_type == ElementType.Table
    assert e.message == "Invalid Get Request: ByLabel Child: Row"
    assert not e.is_insert
    assert e.child == c
    assert e.access == Access.ByLabel
    assert e.metadata == ("abc",)
