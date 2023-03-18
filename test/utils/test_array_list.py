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

        # some operations on empty ArrayList
        al.clear()
        assert al is not None
        assert al == iter(al)
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


