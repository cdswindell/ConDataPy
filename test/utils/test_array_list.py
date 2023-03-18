from __future__ import annotations

import pytest

from ..test_base import TestBase

from cdspy.utils import ArrayList


class TestArrayList(TestBase):
    def test_creation(self) -> None:
        # basic state
        al = ArrayList[int]()
        assert al is not None
        assert not al
        assert len(al) == 0
        assert al.capacity == 0

        # operations on empty ArrayList
        al.clear()
        assert al is not None
        assert al == iter(al)
        with pytest.raises(StopIteration):
            assert next(al)
