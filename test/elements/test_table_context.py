from __future__ import annotations

from cdspy.elements import ElementType
from cdspy.elements import Property
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

    # verify all default values have been set
    for p in ElementType.TableContext.initializable_properties():
        assert dtc.get_property(p) == _TABLE_CONTEXT_DEFAULTS.get(p)
    for p in _TABLE_CONTEXT_DEFAULTS:
        assert dtc.has_property(p)


def test_default_table_context_tags() -> None:
    dtc = TableContext.generate_default_table_context()
    assert dtc

    # verify there are no tags
    assert not dtc.has_property(Property.Tags)
    assert dtc._tags is None
    assert dtc.tags is None

    # getting string tags should not initialize tags property
    assert not dtc.has_property(Property.Tags)
    assert dtc._tags is None

    # create a tag
    t = dtc.to_canonical_tag("abc")
    assert t

    # verify there are now tags
    assert dtc.has_property(Property.Tags)
    assert dtc._tags
    assert dtc.tags  # type: ignore

    # verify there is only one tag, and it is as expected
    assert len(dtc._tags) == 1
    assert dtc.tags == ["abc"]

    # assert that if we add the same tag again, we get the same object (Canonicalization)
    assert dtc.to_canonical_tag("abc") == t
    assert id(dtc.to_canonical_tag("abc")) == id(t)
    assert len(dtc._tags) == 1
    assert dtc.tags == ["abc"]

    # really testing the tag library, but confirm tags are lower-cased and trimmed
    assert dtc.to_canonical_tag(" AbC") == t
    assert id(dtc.to_canonical_tag("ABC")) == id(t)
    assert len(dtc._tags) == 1
    assert dtc.tags == ["abc"]

    # verify we can add additional tags
    assert dtc.to_canonical_tag("ghi")
    assert dtc.to_canonical_tag("def")
    assert len(dtc._tags) == 3
    assert dtc.tags == ["abc", "def", "ghi"]


def test_template_contexts() -> None:
    tc = TableContext(TableContext.generate_default_table_context())
    assert tc

    # assert the new context is not the default
    assert not tc.is_default
    assert tc != TableContext.generate_default_table_context()
    assert tc != TableContext()

    # assert that if we create a second context from a template, it is different
    assert TableContext(TableContext.generate_default_table_context()) != tc

    # assert all initializable properties are defined, and the same as the default context
    for p in ElementType.TableContext.initializable_properties():
        assert tc.get_property(p) == TableContext().get_property(p)
        assert tc.get_property(p) == _TABLE_CONTEXT_DEFAULTS.get(p)
    for p in _TABLE_CONTEXT_DEFAULTS:
        assert tc.has_property(p)

    # change a few initializable properties and confirm that if we make a
    # new context using tc as a template, the new one has the same defaults
    tc.row_capacity_incr = 32
    assert tc.get_property(Property.RowCapacityIncr) == 32

    tc.is_auto_recalculate = False
    assert not tc.get_property(Property.IsAutoRecalculate)

    tc.free_space_threshold = 4.0
    assert tc.get_property(Property.FreeSpaceThreshold) == 4.0

    tc.display_format = "Value: {value}"
    assert tc.get_property(Property.DisplayFormat) == "Value: {value}"

    # make a new context using tc as the template
    ntc = TableContext(tc)
    assert ntc
    assert ntc != tc
    assert ntc != TableContext()

    # verify the new context has tc's default
    for p in ElementType.TableContext.initializable_properties():
        assert ntc.get_property(p) == tc.get_property(p)

    # and that the modified properties don't match the default
    for p in [
        Property.RowCapacityIncr,
        Property.IsAutoRecalculate,
        Property.FreeSpaceThreshold,
        Property.DisplayFormat,
    ]:
        assert ntc.get_property(p) != TableContext().get_property(p)

    # and verify the alternate access methods also check
    assert ntc.row_capacity_incr == 32
    assert ntc.row_capacity_incr == tc.row_capacity_incr

    assert not ntc.is_auto_recalculate
    assert ntc.is_auto_recalculate == tc.is_auto_recalculate

    assert ntc.free_space_threshold == 4.0
    assert ntc.free_space_threshold == tc.free_space_threshold

    assert ntc.display_format == "Value: {value}"
    assert ntc.display_format == tc.display_format
