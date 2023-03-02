from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.elements import Property
from cdspy.exceptions import InvalidPropertyException

from ..elements.test_base_element import MockBaseElement


def test_new_invalid_property_exception_with_arg() -> None:
    r = MockBaseElement(ElementType.Row)
    c = InvalidPropertyException(r)
    assert c
    assert type(c) == InvalidPropertyException
    assert c.element_type == ElementType.Row
    assert c.message == "Property not specified"


def test_invalid_property_exception_with_all_args() -> None:
    me = MockBaseElement(ElementType.Table)
    key = Property.RowCapacityIncr
    c = InvalidPropertyException(me, key)
    assert c
    assert type(c) == InvalidPropertyException
    assert c.element_type == ElementType.Table
    assert c.message == f"Invalid: Table->{key.name}"


def test_invalid_property_exception_with_str_key() -> None:
    me = MockBaseElement(ElementType.Table)
    key = "my property"
    c = InvalidPropertyException(me, key)
    assert c
    assert type(c) == InvalidPropertyException
    assert c.element_type == ElementType.Table
    assert c.message == f"Invalid: {me.element_type.name}->'{key}'"

    me = MockBaseElement(ElementType.Group)
    key = "my group property"
    c = InvalidPropertyException(me, key)
    assert c
    assert type(c) == InvalidPropertyException
    assert c.element_type == ElementType.Group
    assert c.message == f"Invalid: Group->'{key}'"
