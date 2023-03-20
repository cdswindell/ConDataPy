from __future__ import annotations

import pytest

from ..test_base import TestBase

from cdspy.elements import ElementType
from cdspy.elements import Property
from cdspy.elements import Access
from cdspy.elements import default_table_context
from cdspy.elements import TableContext
from cdspy.elements.table_context import _TABLE_CONTEXT_DEFAULTS

from cdspy.exceptions import InvalidAccessException


# noinspection PyMethodMayBeStatic
class TestTableContext(TestBase):
    # ---------------------------
    # Method level setup/teardown
    # ---------------------------
    def setup_method(self) -> None:
        """Make sure default context is empty"""
        TableContext().clear()

    def teardown_method(self) -> None:
        """Clear default context"""
        TableContext().clear()

    def test_default_table_context(self) -> None:
        dtc = default_table_context()
        assert dtc
        assert dtc.is_initialized

        # creating a new instance of TableContext should return same object
        assert TableContext() == dtc

        # object should be "Truthy"
        assert bool(dtc)
        assert dtc

        # check element_type
        assert dtc.element_type == ElementType.TableContext

        # validate it is set as default
        assert dtc.is_default

        # there should be no tables at this point
        assert dtc.num_tables == 0

        # should be null
        assert dtc.is_null

        # verify all default values have been set
        for p in ElementType.TableContext.initializable_properties():
            assert dtc.get_property(p) == _TABLE_CONTEXT_DEFAULTS.get(p)
        for p in _TABLE_CONTEXT_DEFAULTS:
            assert dtc.has_property(p)

    def test_default_table_context_tags(self) -> None:
        dtc = default_table_context()
        assert dtc

        # verify there are no tags
        assert not dtc.has_property(Property.Tags)
        assert not dtc._tags
        assert not dtc.tags

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

    def test_template_contexts(self) -> None:
        tc = TableContext(default_table_context())
        assert tc
        assert tc.is_initialized

        # object should be "Truthy"
        assert bool(tc)
        assert tc

        # assert the new context is not the default
        assert not tc.is_default
        assert tc != default_table_context()
        assert tc != TableContext()

        # assert that if we create a second context from a template, it is different
        assert TableContext(default_table_context()) != tc

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

    def test_nonpersistent_tables_removed_on_dereference(self) -> None:
        tc = TableContext()
        assert tc

        # make two tables
        t1 = tc.create_table()
        assert t1
        assert t1.is_valid
        assert not t1.is_invalid
        assert not t1.is_persistent

        t2 = tc.create_table()
        assert t2
        assert not t1.is_persistent

        assert len(tc) == 2

        # dereferencing t1 remove it from tc
        t1 = None  # type: ignore
        assert len(tc) == 1

    def test_persistent_tables_not_removed_on_dereference(self) -> None:
        tc = TableContext()
        assert tc

        # make two tables
        t1 = tc.create_table()
        assert t1
        assert t1.is_valid
        assert not t1.is_invalid
        assert not t1.is_persistent

        t2 = tc.create_table()
        assert t2
        assert not t2.is_persistent
        assert len(tc) == 2

        # Mark t2 as persistent and dereference
        t2.is_persistent = True
        assert t2.is_persistent
        assert len(tc) == 2
        t2 = None  # type: ignore

        # table should remain in tc
        assert len(tc) == 2

    def test_clear_tables(self) -> None:
        tc = TableContext()
        assert tc
        assert tc.is_default
        # assert tc.is_null
        assert bool(tc)
        assert len(tc) == 0

        # make two tables
        t1 = tc.create_table()
        assert t1
        assert not t1.is_persistent

        t2 = tc.create_table()
        assert t2
        assert not t2.is_persistent
        t2.is_persistent = True
        assert t2.is_persistent

        # there should now be 2 tables
        assert len(tc) == 2
        assert not tc.is_null  # type: ignore

        # clear the table context
        tc.clear()  # type: ignore

        # there should now be no tables
        assert len(tc) == 0
        assert tc.is_null  # type: ignore

        # the created tables should be marked invalid
        assert t1.is_invalid  # type: ignore
        assert t2.is_invalid

    def test_get_tables(self) -> None:
        tc = TableContext()
        assert tc
        assert tc.is_null

        # create several tables with different attributes
        t1 = tc.create_table()
        assert t1
        t1.label = "table 1"
        t1.set_property("my_key", "my_string")
        t1.tag("ghi")
        assert t1.has_any_tags("abc", "def", "ghi")

        t2 = tc.create_table()
        assert t2
        t2.is_persistent = True
        t2.tag("abc", "def", "ghi")
        assert t2.has_all_tags("abc", "def", "ghi")
        t2.description = "t2 Description"

        t3 = tc.create_table(32, 4)
        assert t3
        assert t3.uuid
        assert t2.ident
        assert len(tc) == 3

        # retrieve by label
        assert tc.get_table(Access.ByLabel, "table 1") == t1
        assert tc.get_table(Access.ByLabel, "table 1") != t2
        assert tc.get_table(Access.ByLabel, "table 1") != t3

        # retrieve string property
        assert tc.get_table(Access.ByProperty, "my_key", "my_string") == t1
        assert not tc.get_table(Access.ByProperty, "no_key", "my_string")

        # retrieve by uuid
        assert tc.get_table(Access.ByUUID, t3.uuid) == t3
        assert tc.get_table(Access.ByUUID, t2.uuid) != t3

        # retrieve by tags
        assert tc.get_table(Access.ByTags, "abc") == t2
        assert tc.get_table(Access.ByTags, "abc", "def", "ghi") == t2
        assert tc.get_table(Access.ByTags, "ghi") in (t1, t2)
        assert tc.get_table(Access.ByTags, "ghi") != t3
        assert not tc.get_table(Access.ByTags, "xyz")

        # retrieve by tags via Property
        assert tc.get_table(Access.ByProperty, Property.Tags, "abc") == t2
        assert tc.get_table(Access.ByProperty, Property.Tags, "abc", " dEf  ", "ghi") == t2
        assert tc.get_table(Access.ByProperty, Property.Tags, "ghi") in (t1, t2)
        assert tc.get_table(Access.ByProperty, Property.Tags, "GHI") != t3
        assert not tc.get_table(Access.ByProperty, Property.Tags, "xyz")

        # retrieve by reference
        assert tc.get_table(Access.ByReference, t1) == t1
        assert tc.get_table(Access.ByReference, t2) == t2
        assert tc.get_table(Access.ByReference, t3) == t3

        # retrieve by ident
        assert tc.get_table(Access.ByIdent, t1.ident) == t1
        assert tc.get_table(Access.ByIdent, t2.ident) == t2
        assert tc.get_table(Access.ByIdent, t3.ident) == t3

        # retrieve by description
        assert tc.get_table(Access.ByDescription, "t2 Description") == t2
        assert tc.get_table(Access.ByDescription, "t2 Description") != t1

        # test that unsupported access methods raise exception
        for access in [Access.First, Access.Last, Access.Next, Access.Previous, Access.Current, Access.ByIndex]:
            with pytest.raises(InvalidAccessException, match=f"Invalid Get Request: {access.name} Child: Table"):
                assert tc.get_table(access)
