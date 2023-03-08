from __future__ import annotations

import pytest

from cdspy.elements import Tag


def test_tag() -> None:
    t = Tag("TheTag")
    assert t
    assert t.label == "thetag"

    assert isinstance(t, Tag)

    # test multiple spaces are reduced to a single space
    t = Tag(" The     Tag  ")
    assert t
    assert t.label == "the tag"

    # test __str__
    assert str(t) == "[Tag: 'the tag']"

    # test __repr__
    assert repr(t) == "Tag('the tag')"

    # test equality and __repr__; tags are normalized to lower case
    assert eval(repr(t)) == t
    assert Tag("  THE   TAG") == t
    assert Tag("c") != t
    assert t != 42

    # test lt
    smaller_tag = Tag("abc")
    assert smaller_tag
    assert smaller_tag < t
    if t < smaller_tag:
        assert False

    with pytest.raises(NotImplementedError):
        assert t < 42  # type: ignore

    # test gt
    larger_tag = Tag("zyx")
    assert larger_tag
    assert larger_tag > t
    if t > larger_tag:
        assert False

    with pytest.raises(NotImplementedError):
        assert t > 42  # type: ignore

    # test hash
    assert hash(t) == hash("the tag")

    # test set membership
    t2 = Tag("the tag")
    s = {t, t2}
    assert len(s) == 1

    # test should fail; tags are immutable
    with pytest.raises(AttributeError):
        t.label = "foo"  # type: ignore

    # verify Tag class is frozen and that new attributes cannot be set
    with pytest.raises(AttributeError, match="'Tag' object has no attribute 'foo'"):
        t.foo = "foo"  # type: ignore


def test_as_strings() -> None:
    s = {Tag("d"), Tag("a"), Tag("b"), Tag("d"), Tag("c"), Tag("d")}
    assert s
    assert len(s) == 4
    assert Tag.as_labels(s) == ["a", "b", "c", "d"]

    # test that set containing non-tags is handled
    s = {Tag("d"), Tag("a"), 42, Tag("b"), Tag("d"), Tag("c"), Tag("d"), "d"}  # type: ignore
    assert s
    assert len(s) == 6
    assert Tag.as_labels(s) == ["a", "b", "c", "d"]
