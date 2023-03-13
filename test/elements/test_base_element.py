from __future__ import annotations

from typing import Any, cast, Dict, Collection, TYPE_CHECKING

import pytest

from cdspy.elements import ElementType
from cdspy.elements import BaseElementState
from cdspy.elements import Property

from cdspy.elements.base_element import TABLE_PROPERTIES_KEY
from cdspy.elements.base_element import BaseElement

from cdspy.exceptions import InvalidPropertyException
from cdspy.exceptions import ReadOnlyException
from cdspy.exceptions import UnimplementedException

if TYPE_CHECKING:
    from cdspy.elements import T


# create test class from BaseElement
class MockBaseElement(BaseElement):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self._element_type = self._parse_args(ElementType, "et", 0, ElementType.Table, *args, **kwargs)
        if self._parse_args(bool, "initialized", 1, True, *args, **kwargs):
            self._set_initialized()

    @property
    def element_type(self) -> ElementType:
        return self._element_type  # type: ignore

    @property
    def is_null(self) -> bool:
        return False

    def _iter_objs(self) -> Collection[T]:
        return cast(Collection[T], [self])


def test_base_element_initial_state() -> None:
    tc = MockBaseElement(initialized=False)
    assert tc
    assert tc._state == BaseElementState.IS_INITIALIZING_FLAG
    assert tc.is_initializing
    assert not tc.is_initialized
    assert vars(tc).get(TABLE_PROPERTIES_KEY) is None
    assert tc._element_properties() is None

    # retest, but mark element as initialized
    tc = MockBaseElement()
    assert tc
    assert tc._state == BaseElementState.NO_FLAGS_SET
    assert not tc.is_initializing
    assert tc.is_initialized


def test_set_reset_element_properties() -> None:
    tc = MockBaseElement()
    assert tc
    assert tc._element_properties() is None

    assert tc._set_property("a test prop", 42) is None
    assert tc._set_property("a test prop", 43) == 42

    tc._reset_element_properties()
    assert tc._element_properties() is None


def test_clear_property() -> None:
    tc = MockBaseElement(ElementType.Cell)
    assert tc

    assert tc._element_properties() is None
    assert not tc.has_property(Property.Label)
    assert not tc._clear_property(Property.Label)

    # set a property on the cell
    assert tc._set_property(Property.Label, "Test Label") is None
    assert tc.has_property(Property.Label)
    assert tc._element_properties() is not None
    assert cast(Dict[Property, str], tc._element_properties())[Property.Label] == "Test Label"

    # clear the property, should return True
    assert tc._clear_property(Property.Label)
    assert not tc.has_property(Property.Label)
    assert Property.Label not in cast(Dict[Property, Any], tc._element_properties())

    # test a read-only property cannot be cleared
    rop = tc.element_type.read_only_properties()
    assert rop
    for p in rop:
        with pytest.raises(ReadOnlyException, match=f"ReadOnly: Cell->{p.name}"):
            assert tc._clear_property(p)

    # test with string key
    key = "str key"
    assert not tc.has_property(key)
    assert not tc._clear_property(key)

    # set a str property on the cell
    assert tc._set_property(key, "Test Str") is None
    assert tc.has_property(key)
    assert cast(Dict[str, str], tc._element_properties())[key] == "Test Str"

    # clear the property, should return True
    assert tc._clear_property(key)
    assert not tc.has_property(key)
    assert key not in cast(Dict[str, str], tc._element_properties())


def test_vet_property_key() -> None:
    # create a mock table
    tc = MockBaseElement()
    assert tc

    # test that null key raises error
    with pytest.raises(InvalidPropertyException, match="Property not specified"):
        tc._vet_property_key(None)  # type: ignore

    # test that string keys are valid and are returned normalized
    assert tc._vet_property_key("  this   is a   str KEY ") == "this is a str key"  # type: ignore

    # test that mutable table properties are allowed
    for p in tc.element_type.mutable_properties():
        assert tc._vet_property_key(p, for_mutable_op=True) == p  # type: ignore

    # test that read-only table properties are not allowed
    for p in tc.element_type.read_only_properties():
        with pytest.raises(ReadOnlyException, match=f"ReadOnly: Table->{p.name}"):
            assert tc._vet_property_key(p, for_mutable_op=True) == p  # type: ignore

    # test that unsupported properties are not allowed
    ap = set(Property)  # all properties
    assert ap

    tp = set(tc.element_type.properties())  # all table properties
    assert tp

    nsp = tp.symmetric_difference(ap)
    assert nsp

    # test that properties not supported by a table are not allowed
    for p in nsp:
        with pytest.raises(UnimplementedException, match=f"Unimplemented: Table->{p.name}"):
            assert tc._vet_property_key(p, for_mutable_op=True) == p  # type: ignore

    # test that key types other than None, str, and Property are not allowed
    key = 42
    with pytest.raises(InvalidPropertyException, match=f"Invalid Property: {type(key)}"):
        assert tc._vet_property_key(key) == key  # type: ignore

    key = object()  # type: ignore
    with pytest.raises(InvalidPropertyException, match=f"Invalid Property: {type(key)}"):
        assert tc._vet_property_key(key, for_mutable_op=True) == key  # type: ignore


def test_set_property() -> None:
    # create a mock table context
    tc = MockBaseElement(ElementType.TableContext)
    assert tc

    # verify read-only properties can not be set
    with pytest.raises(ReadOnlyException, match="ReadOnly: TableContext->Tags"):
        assert tc._set_property(Property.Tags, 42) == 42  # type: ignore

    # verify unsupported properties can not be set
    with pytest.raises(UnimplementedException, match="Unimplemented: TableContext->Derivation"):
        assert tc._set_property(Property.Derivation, 42) == 42  # type: ignore

    # verify supported properties can be set
    assert tc._set_property(Property.Description, 42) is None

    # and if reset, return the previously set value
    assert tc._set_property(Property.Description, 43) == 42

    # verify string properties can be set
    assert tc._set_property("String Key", "String 1") is None

    # and if reset, return the previously set value
    assert tc._set_property("string key", 43) == "String 1"

    # fail if key is null
    with pytest.raises(InvalidPropertyException, match="Property not specified"):
        assert tc._set_property(None, 33) == 42  # type: ignore

    # fail if key is not a string or property
    with pytest.raises(InvalidPropertyException, match=f"Invalid Property: {type(42)}"):
        assert tc._set_property(42, 42) == 42  # type: ignore


def test_get_property() -> None:
    # create a mock cell
    tc = MockBaseElement(ElementType.Cell)
    assert tc

    # verify properties aren't present if not set
    assert tc.get_property(Property.Label) is None
    assert tc.get_property(Property.DataType) is None
    assert tc.get_property("string  Key") is None
    assert tc.get_property("Universal Answer") is None

    # set some values
    tc._set_property(Property.Label, "test label")
    tc._set_property(Property.DataType, int)
    tc._set_property("String Key", "test string")
    tc._set_property("Universal Answer", 42)

    # verify that these elements can be returned
    assert tc.get_property(Property.Label) == "test label"
    assert tc.get_property(Property.DataType) == int
    assert tc.get_property("string  Key") == "test string"
    assert tc.get_property("Universal Answer") == 42

    # verify properties aren't present if not set
    assert tc.get_property(Property.Description) is None
    assert tc.get_property("unset key") is None

    # fail if retrieving an unsupported property
    with pytest.raises(UnimplementedException, match="Unimplemented: Cell->RowCapacityIncr"):
        assert tc.get_property(Property.RowCapacityIncr) == 42  # type: ignore

    # fail if key is null
    with pytest.raises(InvalidPropertyException, match="Property not specified"):
        assert tc.get_property(None) == 42  # type: ignore

    # fail if key is not a string or property
    with pytest.raises(InvalidPropertyException, match=f"Invalid Property: {type(42)}"):
        assert tc.get_property(42) == 42  # type: ignore
