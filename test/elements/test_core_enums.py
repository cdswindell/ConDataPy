from __future__ import annotations

from typing import Optional

import pytest

from cdspy.elements import ElementType
from cdspy.elements import Property


def test_element_type_basic() -> None:
    assert ElementType.Column.nickname == "Col"
    assert ElementType.Row.nickname == "Row"
    assert len(ElementType) == 7
    assert ElementType["TableContext"] == ElementType.TableContext
    assert ElementType["Table"] == ElementType.Table
    assert ElementType["Row"] == ElementType.Row
    assert ElementType["Column"] == ElementType.Column
    assert ElementType["Cell"] == ElementType.Cell
    assert ElementType["Group"] == ElementType.Group
    assert ElementType["Derivation"] == ElementType.Derivation


def test_element_type_properties() -> None:
    for et in ElementType:
        assert et
        for p in et.properties():
            assert p
            assert p.is_implemented_by(et)
            if p.is_required_property:
                assert p in et.required_properties()
                assert p not in et.optional_properties()
            if p.is_optional_property:
                assert p not in et.required_properties()
                assert p in et.optional_properties()
            if p.is_read_only_property:
                assert p in et.read_only_properties()
                assert p not in et.mutable_properties()
            if p.is_mutable_property:
                assert p not in et.read_only_properties()
                assert p in et.mutable_properties()
            if p.is_initializable_property:
                assert p in et.initializable_properties()


def test_property_basic() -> None:
    # test enum value class
    for p in Property:
        assert p.name
        assert p.value
        assert p.value.__class__.__name__ == "_TablePropertyInfo"
        assert hash(p) == hash(p.name)
        assert hash(p) != p
        v = p.value
        assert str(v).startswith("[optional") or str(v).startswith("[required")
        assert str(p.value).endswith("]") or str(p.value).startswith(", read-only]")


def test_property_getters() -> None:
    # test dict
    assert Property["Label"] == Property.Label
    assert Property.Label in Property
    assert Property.Label.nickname is not None

    # test callable
    assert Property("Label") == Property.Label

    for p in Property:
        assert Property[p.name] == p
        assert Property(p.name) == p
        assert Property.by_name(p.name) == p
        assert Property.by_name(" " + p.name + "  ") == p
        assert Property.by_name(p.name.lower()) == p
        assert Property.by_name(p.name.upper()) == p
        assert Property.by_attr_name(p.as_attr_name) == p
        unique_nicknames = set()
        if p.nickname:
            # test that nicknames are unique
            assert p.nickname.lower() not in unique_nicknames
            unique_nicknames.add(p.nickname.lower())
            assert Property.by_nickname(p.nickname) == p
            assert Property.by_nickname("  " + p.nickname + "  ") == p
            assert Property.by_nickname(p.nickname.lower()) == p
            assert Property.by_nickname(p.nickname.upper()) == p
        else:
            assert Property.by_nickname(p.nickname) is None
            assert Property.by_nickname(p.nickname.lower()) is None
            assert Property.by_nickname(p.nickname.upper()) is None

    # test a handful of as_attr_name examples
    assert Property.Tags.as_attr_name == "tags"
    assert Property.by_attr_name("tags") == Property.Tags
    assert Property.AreTablesPersistent.as_attr_name == "are_tables_persistent"
    assert Property.by_attr_name("are_tables_persistent") == Property.AreTablesPersistent
    assert Property.IsPendingAllowCoreThreadTimeout.as_attr_name == "is_pending_allow_core_thread_timeout"


def test_property_getter_failures() -> None:
    assert Property.by_nickname() is None
    assert Property.by_nickname("") is None
    assert Property.by_nickname("  ") is None
    assert Property.by_nickname(" nickname not present ") is None

    # test Property.by_name()
    with pytest.raises(ValueError, match="None/Empty is not a valid Property"):
        Property.by_name("  ")

    with pytest.raises(ValueError, match="'nickname not present' is not a valid Property"):
        Property.by_name(" nickname not present ")

    with pytest.raises(ValueError) as e_info:
        Property.by_name(" nickname not present ")
    assert e_info is not None
    assert e_info.value.args[0] == "'nickname not present' is not a valid Property"
    assert str(e_info.value) == "'nickname not present' is not a valid Property"

    # test Property() and Property._missing_()
    with pytest.raises(ValueError, match="'nickname not present' is not a valid Property"):
        Property(" nickname not present ")

    with pytest.raises(ValueError) as e_info:
        Property(" nickname not present ")
    assert e_info is not None
    assert e_info.value.args[0] == "'nickname not present' is not a valid Property"
    assert str(e_info.value) == "'nickname not present' is not a valid Property"


def test_property_lt_oper() -> None:
    spl = sorted(Property)
    assert spl is not None
    prev_prop: Optional[Property] = None  # make mypy happy
    for p in spl:
        if prev_prop:
            assert prev_prop < p
        prev_prop = p

    # test negative case
    with pytest.raises(NotImplementedError):
        assert Property.Label < "Label"  # type: ignore


def test_property_gt_oper() -> None:
    spl = sorted(Property)
    assert spl is not None
    prev_prop: Optional[Property] = None
    for p in spl:
        if prev_prop:
            assert p > prev_prop
        prev_prop = p

    # test negative case
    with pytest.raises(NotImplementedError):
        assert Property.Label > "Label"  # type: ignore


def test_is_implemented_by() -> None:
    for p in Property:
        assert p.is_implemented_by(None) is not None
        assert not p.is_implemented_by(None)

        assert p.is_implemented_by(42) is not None  # type: ignore
        assert not p.is_implemented_by(42)  # type: ignore
        for et in ElementType:
            r = p.is_implemented_by(et)
            assert r is not None
            if et in p.value._implemented_by:
                assert r
            else:
                assert not r


def test_property_flags() -> None:
    for p in Property:
        if p.value._read_only:
            assert p.is_read_only_property
            assert not p.is_mutable_property
        else:
            assert not p.is_read_only_property
            assert p.is_mutable_property

        if p.value._optional:
            assert not p.is_required_property
            assert p.is_optional_property
        else:
            assert p.is_required_property
            assert not p.is_optional_property

        if p.value._initializable:
            assert p.is_initializable_property
        else:
            assert not p.is_initializable_property
