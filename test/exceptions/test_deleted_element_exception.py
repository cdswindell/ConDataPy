from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.exceptions import DeletedElementException


def test_new_deleted_element_exception() -> None:
    c = DeletedElementException()
    assert c
    assert type(c) == DeletedElementException
    assert c.element_type() is None
    assert c.message() == "Operations on deleted elements are not allowed"


def test_new_deleted_element_exception_with_arg() -> None:
    c = DeletedElementException(ElementType.Row)
    assert c
    assert type(c) == DeletedElementException
    assert c.element_type() == ElementType.Row
    assert c.message() == "Operations on deleted Rows are not allowed"


def test_new_deleted_element_exception_with_named_arg() -> None:
    c = DeletedElementException(message="Custom Message")
    assert c
    assert type(c) == DeletedElementException
    assert c.element_type() is None
    assert c.message() == "Custom Message"


def test_new_deleted_element_exception_with_args() -> None:
    c = DeletedElementException(ElementType.Group, "Test Message")
    assert c
    assert type(c) == DeletedElementException
    assert c.element_type() == ElementType.Group
    assert c.message() == "Test Message"
