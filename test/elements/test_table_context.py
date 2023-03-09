from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.elements import TableContext
from cdspy.elements.table_context import _TABLE_CONTEXT_DEFAULTS


def test_default_table_context() -> None:
    dtc = TableContext.generate_default_table_context()
    assert dtc

    # creating a new instance of TableContext should return same object
    assert TableContext() == dtc

    # check element_type
    assert dtc.element_type == ElementType.TableContext

    # validate it is set as default
    assert dtc.is_default

    # there should be no tables at this point
    assert dtc.num_tables == 0

    # should be null
    assert dtc._is_null

    # verify default values have been set
    for p in ElementType.TableContext.initializable_properties():
        assert dtc.get_property(p) == _TABLE_CONTEXT_DEFAULTS.get(p)
