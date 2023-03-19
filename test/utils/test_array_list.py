from __future__ import annotations

import pytest

from ..test_base import TestBase

from cdspy.utils import ArrayList
from cdspy.utils.array_list import _DEFAULT_CAPACITY


class TestArrayList(TestBase):
    def test_creation(self) -> None:
        # basic state
        al = ArrayList[int]()
        assert al is not None
        assert not al
        assert len(al) == 0
        assert al.capacity == 0
        assert al.capacity_increment == _DEFAULT_CAPACITY

        # some operations on empty ArrayList that should not raise exceptions
        al.reverse()
        al.clear()
        assert al.count(0) == 0

        # and some that should
        with pytest.raises(StopIteration):
            assert next(al)
        with pytest.raises(IndexError):
            assert al.pop()
        with pytest.raises(IndexError):
            assert al[0] == 1
        with pytest.raises(IndexError):
            del al[0]
        with pytest.raises(ValueError):
            al.remove(0)
        with pytest.raises(ValueError):
            al.index(0)

    def test_assignment(self) -> None:
        al = ArrayList[int](initial_capacity=32)
        assert al is not None
        assert len(al) == 0
        assert al.capacity == 32

        # assign some values
        al[3] = 14
        al[12] = -2
        assert len(al) == 13
        assert al.capacity == 32

        # test positive and negative indices
        assert al[3] == 14
        assert al[12] == -2

        assert al[-1] == -2
        assert al[-10] == 14

        # test count
        assert al.count(-2) == 1
        assert al.count(14) == 1
        assert al.count(2) == 0

        # and index
        assert al.index(14) == 3
        assert al.index(-2) == 12
        assert al.index(14, -12, -1) == 3
        assert al.index(14, -11, -1) == 3
        assert al.index(14, -10, -1) == 3
        with pytest.raises(ValueError):
            assert al.index(14, -9) == 3
        with pytest.raises(ValueError):
            assert al.index(14, 100) == 3
        with pytest.raises(ValueError):
            assert al.index(14, 0, 2) == 3
        with pytest.raises(ValueError):
            assert al.index(14, -6, 2) == 3

        # slices
        assert al[3:4] == [14]
        assert al[-10:-9] == [14]

        # reverse the list and retest
        al.reverse()
        assert len(al) == 13
        assert al[0] == -2
        assert al[9] == 14
        assert al[al.index(14)] == 14

        # test slice assignment
        al[0:5] = [1, 2]
        assert len(al) == 10
        assert al[6] == 14
        assert al[al.index(14)] == 14
        assert al[1:7] == [2, None, None, None, None, 14]

        # and deletion
        del al[1:5]
        assert len(al) == 6
        assert al[2] == 14
        assert al.pop(-6) == 1
        assert len(al) == 5
        assert al.capacity == 32

        # and remove
        al.remove(14)

        # and clear()
        al.clear()
        assert al.capacity == _DEFAULT_CAPACITY
