from __future__ import annotations

from collections.abc import Iterable, Iterator
import pytest
import re

from ..test_base import TestBase

from cdspy.utils import JustInTimeSet


class MockObject:
    def __init__(self, x: int):
        self._x = x

    @property
    def x(self) -> int:
        return self._x

    def __repr__(self) -> str:
        return f"MockObject({self.x})"

    def __hash__(self) -> int:
        return hash(self._x)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MockObject):
            return False
        return self.x == other.x

    def __lt__(self, other: MockObject) -> bool:
        if not isinstance(other, MockObject):
            raise NotImplementedError
        return self.x < other.x


class TestBasicJustInTimeSet(TestBase):
    def test_create(self) -> None:
        jit = JustInTimeSet[MockObject]()
        assert jit is not None
        assert len(jit) == 0
        assert MockObject(12) not in jit

        # jit should be Falsy because it is empty
        assert not jit

        # iterator should have no more elements
        i = iter(jit)
        assert i
        assert isinstance(jit, Iterable)
        assert not isinstance(jit, Iterator)
        assert isinstance(i, Iterable)
        assert isinstance(i, Iterator)

        with pytest.raises(StopIteration):
            assert next(i)

    def test_weak_references(self) -> None:
        jit = JustInTimeSet[MockObject]()
        assert not jit
        assert len(jit) == 0

        # create some objects and add them to set
        o1 = MockObject(1)
        jit.add(o1)
        assert o1 in jit

        o2 = MockObject(2)
        jit.add(o2)
        assert o2 in jit

        o3 = MockObject(3)
        jit.add(o3)
        assert o3 in jit

        # length should now be 3 and it should be Truthy
        assert len(jit) == 3
        assert jit

        # add o1 again, len should still be 3
        jit.add(o1)
        assert len(jit) == 3
        assert jit
        assert o1 in jit

        # delete o2, it should be removed from jit
        del o2
        assert len(jit) == 2
        assert jit

        # dereferencing should also remove element
        o1 = None  # type: ignore
        assert len(jit) == 1

        # o3 should remain
        assert o3 in jit

        # clear set, none should remain
        jit.clear()
        assert not jit
        assert len(jit) == 0

    def test_iteration(self) -> None:
        jit = JustInTimeSet[MockObject]()
        assert not jit
        assert len(jit) == 0

        # create some objects and add them to set
        o1 = MockObject(1)
        o2 = MockObject(2)
        o3 = MockObject(3)
        jit.update([o1, o2, o3])

        # length should now be 3 and it should be Truthy
        assert len(jit) == 3
        assert jit

        # test pop
        while jit:
            assert jit.pop()
        assert not jit
        assert len(jit) == 0

        # remake jit
        jit.update([o1, o2, o3])
        assert len(jit) == 3
        assert jit

        # test iterator via "in"
        for x in jit:
            assert x
            assert x in [o1, o2, o3]

        # test iterator via iter() and next()
        i = iter(jit)
        assert i
        assert isinstance(i, Iterable)
        assert isinstance(i, Iterator)

        # test next (1)
        o = next(i)
        assert o
        assert o in jit

        # test next (2)
        o = next(i)
        assert o
        assert o in jit

        # test next (3)
        o = next(i)
        assert o
        assert o in jit

        # next call to next() raises exception
        with pytest.raises(StopIteration):
            assert next(i)

    def test_discard(self) -> None:
        jit = JustInTimeSet[MockObject]()
        assert not jit
        assert len(jit) == 0

        # create some objects and add them to set
        o1 = MockObject(1)
        o2 = MockObject(2)
        o3 = MockObject(3)
        jit.update([o1, o2, o3])

        # length should now be 3 and it should be Truthy
        assert len(jit) == 3
        assert jit

        # discard an element in the set, should be no exception
        jit.discard(o2)
        assert o2 not in jit
        assert len(jit) == 2

        # discard an element not in the set, should be no exception
        jit.discard(MockObject(100))
        assert len(jit) == 2

        # test the condition when the backing set hasn;t been created
        jit = JustInTimeSet()
        assert not jit

        jit.discard(o2)
        jit.discard(MockObject(100))
        assert not jit
        assert len(jit) == 0

    def test_remove(self) -> None:
        jit = JustInTimeSet[MockObject]()
        assert not jit
        assert len(jit) == 0

        # create some objects and add them to set
        o1 = MockObject(1)
        o2 = MockObject(2)
        o3 = MockObject(3)
        jit.update([o1, o2, o3])

        # length should now be 3 and it should be Truthy
        assert len(jit) == 3
        assert jit

        # remove an element in the set, should be no exception
        jit.remove(o2)
        assert o2 not in jit
        assert len(jit) == 2

        # discard an element not in the set, should be no exception
        with pytest.raises(KeyError):
            jit.remove(MockObject(100))
        assert len(jit) == 2

        # test the condition when the backing set hasn;t been created
        jit = JustInTimeSet()
        assert not jit

        with pytest.raises(KeyError):
            jit.remove(o2)

        with pytest.raises(KeyError, match=re.escape("MockObject(100)")):
            jit.remove(MockObject(100))
        assert not jit
        assert len(jit) == 0
