from __future__ import annotations

from typing import Optional

import pytest

from cdspy.elements import ElementType, BaseElementState
from cdspy.elements import Property

from cdspy.elements import _ENFORCE_DATATYPE_FLAG
from cdspy.elements import _READONLY_FLAG
from cdspy.elements import _SUPPORTS_NULL_FLAG
from cdspy.elements import _PENDING_THREAD_POOL_FLAG
from cdspy.elements import _IN_USE_FLAG
from cdspy.elements import _HAS_CELL_VALIDATOR_FLAG
from cdspy.elements import _AUTO_RECALCULATE_DISABLED_FLAG
from cdspy.elements import _AUTO_RECALCULATE_FLAG
from cdspy.elements import _IS_DERIVED_CELL_FLAG
from cdspy.elements import _IS_PENDING_FLAG
from cdspy.elements import _IS_AWAITING_FLAG
from cdspy.elements import _ROW_LABELS_INDEXED_FLAG
from cdspy.elements import _COLUMN_LABELS_INDEXED_FLAG
from cdspy.elements import _CELL_LABELS_INDEXED_FLAG
from cdspy.elements import _TABLE_LABELS_INDEXED_FLAG
from cdspy.elements import _GROUP_LABELS_INDEXED_FLAG
from cdspy.elements import _IS_TABLE_PERSISTENT_FLAG
from cdspy.elements import _EVENTS_NOTIFY_IN_SAME_THREAD_FLAG
from cdspy.elements import _EVENTS_ALLOW_CORE_THREAD_TIMEOUT_FLAG
from cdspy.elements import _PENDINGS_ALLOW_CORE_THREAD_TIMEOUT_FLAG
from cdspy.elements import _IS_DEFAULT_FLAG
from cdspy.elements import _IS_DIRTY_FLAG
from cdspy.elements import _HAS_CELL_ERROR_MSG_FLAG
from cdspy.elements import _IS_INVALID_FLAG
from cdspy.elements import _IS_PROCESSED_FLAG

from cdspy.elements.base_element import TABLE_PROPERTIES_KEY
from cdspy.elements.base_element import BaseElement

from cdspy.exceptions import InvalidPropertyException
from cdspy.exceptions import ReadOnlyException
from cdspy.exceptions import UnimplementedException


# create test class from BaseElement
class MockBaseElement(BaseElement):
    def __init__(self, et: Optional[ElementType] = ElementType.Table) -> None:
        super().__init__()
        self._element_type = et

    def element_type(self) -> ElementType:
        return self._element_type  # type: ignore

    def _is_null(self) -> bool:
        return False


def test_base_element_state_flags() -> None:
    assert _ENFORCE_DATATYPE_FLAG == BaseElementState.ENFORCE_DATATYPE_FLAG
    assert _READONLY_FLAG == BaseElementState.READONLY_FLAG
    assert _SUPPORTS_NULL_FLAG == BaseElementState.SUPPORTS_NULL_FLAG
    assert _PENDING_THREAD_POOL_FLAG == BaseElementState.PENDING_THREAD_POOL_FLAG
    assert _IN_USE_FLAG == BaseElementState.IN_USE_FLAG
    assert _HAS_CELL_VALIDATOR_FLAG == BaseElementState.HAS_CELL_VALIDATOR_FLAG
    assert _HAS_CELL_ERROR_MSG_FLAG == BaseElementState.HAS_CELL_ERROR_MSG_FLAG
    assert _AUTO_RECALCULATE_DISABLED_FLAG == BaseElementState.AUTO_RECALCULATE_DISABLED_FLAG
    assert _AUTO_RECALCULATE_FLAG == BaseElementState.AUTO_RECALCULATE_FLAG
    assert _IS_PENDING_FLAG == BaseElementState.IS_PENDING_FLAG
    assert _IS_DERIVED_CELL_FLAG == BaseElementState.IS_DERIVED_CELL_FLAG
    assert _IS_AWAITING_FLAG == BaseElementState.IS_AWAITING_FLAG
    assert _ROW_LABELS_INDEXED_FLAG == BaseElementState.ROW_LABELS_INDEXED_FLAG
    assert _COLUMN_LABELS_INDEXED_FLAG == BaseElementState.COLUMN_LABELS_INDEXED_FLAG
    assert _CELL_LABELS_INDEXED_FLAG == BaseElementState.CELL_LABELS_INDEXED_FLAG
    assert _TABLE_LABELS_INDEXED_FLAG == BaseElementState.TABLE_LABELS_INDEXED_FLAG
    assert _GROUP_LABELS_INDEXED_FLAG == BaseElementState.GROUP_LABELS_INDEXED_FLAG
    assert _IS_TABLE_PERSISTENT_FLAG == BaseElementState.IS_TABLE_PERSISTENT_FLAG
    assert _EVENTS_NOTIFY_IN_SAME_THREAD_FLAG == BaseElementState.EVENTS_NOTIFY_IN_SAME_THREAD_FLAG
    assert _EVENTS_ALLOW_CORE_THREAD_TIMEOUT_FLAG == BaseElementState.EVENTS_ALLOW_CORE_THREAD_TIMEOUT_FLAG
    assert _PENDINGS_ALLOW_CORE_THREAD_TIMEOUT_FLAG == BaseElementState.PENDINGS_ALLOW_CORE_THREAD_TIMEOUT_FLAG
    assert _IS_DEFAULT_FLAG == BaseElementState.IS_DEFAULT_FLAG
    assert _IS_DIRTY_FLAG == BaseElementState.IS_DIRTY_FLAG
    assert _IS_INVALID_FLAG == BaseElementState.IS_INVALID_FLAG
    assert _IS_PROCESSED_FLAG == BaseElementState.IS_PROCESSED_FLAG


def test_base_element_initial_state() -> None:
    tc = MockBaseElement()
    assert tc
    assert tc._m_flags == BaseElementState.NO_FLAGS
    assert vars(tc).get(TABLE_PROPERTIES_KEY) is None
    assert tc._element_properties() is None


def test_set_reset_elem_properties() -> None:
    tc = MockBaseElement()
    assert tc
    assert tc._element_properties() is None

    assert tc._set_property("a test prop", 42) is None
    assert tc._set_property("a test prop", 43) == 42

    tc._reset_elem_properties()
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
    assert tc._element_properties()[Property.Label] == "Test Label"

    # clear the property, should return True
    assert tc._clear_property(Property.Label)
    assert not tc.has_property(Property.Label)
    assert Property.Label not in tc._element_properties()

    # test a read-only property cannot be cleared
    rop = tc.element_type().read_only_properties()
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
    assert tc._element_properties()[key] == "Test Str"

    # clear the property, should return True
    assert tc._clear_property(key)
    assert not tc.has_property(key)
    assert key not in tc._element_properties()


def test_vet_key_for_mutable_op() -> None:
    # create a mock table
    tc = MockBaseElement()
    assert tc

    # test that null key raises error
    with pytest.raises(InvalidPropertyException, match="Property not specified"):
        tc._BaseElement__vet_key_for_mutable_op(None)  # type: ignore

    # test that string keys are valid and are returned normalized
    assert tc._BaseElement__vet_key_for_mutable_op("  this   is a   str KEY ") == "this is a str key"  # type: ignore

    # test that mutable table properties are allowed
    for p in tc.element_type().mutable_properties():
        assert tc._BaseElement__vet_key_for_mutable_op(p) == p  # type: ignore

    # test that read-only table properties are not allowed
    for p in tc.element_type().read_only_properties():
        with pytest.raises(ReadOnlyException, match=f"ReadOnly: Table->{p.name}"):
            assert tc._BaseElement__vet_key_for_mutable_op(p) == p  # type: ignore

    # test that unsupported properties are not allowed
    ap = set(Property)  # all properties
    assert ap

    tp = set(tc.element_type().properties())  # all table properties
    assert tp

    nsp = tp.symmetric_difference(ap)
    assert nsp

    # test that properties not supported by a table are not allowed
    for p in nsp:
        with pytest.raises(UnimplementedException, match=f"Unimplemented: Table->{p.name}"):
            assert tc._BaseElement__vet_key_for_mutable_op(p) == p  # type: ignore

    # test that key types other than None, str, and Property are not allowed
    key = 42
    with pytest.raises(AttributeError, match=f"Unsupported property key type: {type(key)}"):
        assert tc._BaseElement__vet_key_for_mutable_op(key) == key  # type: ignore

    key = object()  # type: ignore
    with pytest.raises(AttributeError, match=f"Unsupported property key type: {type(key)}"):
        assert tc._BaseElement__vet_key_for_mutable_op(key) == key  # type: ignore
