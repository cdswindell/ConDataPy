from __future__ import annotations

from ..test_base import TestBase
from cdspy.elements import ElementType
from cdspy.exceptions.base_exceptions import BaseTableException


class TestBaseException(TestBase):
    def test_new_base_table_exception(self) -> None:
        c = BaseTableException()
        assert c
        assert type(c) == BaseTableException
        assert c.element_type is None
        assert c.message is None

    def test_new_base_table_exception_with_arg(self) -> None:
        c = BaseTableException(ElementType.TableContext)
        assert c
        assert type(c) == BaseTableException
        assert c.element_type == ElementType.TableContext
        assert c.message is None

    def test_new_base_table_exception_with_args(self) -> None:
        c = BaseTableException(ElementType.Table, "This is a table")
        assert c
        assert type(c) == BaseTableException
        assert c.element_type == ElementType.Table
        assert c.message == "This is a table"
