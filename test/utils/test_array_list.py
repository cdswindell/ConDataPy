from __future__ import annotations

import pytest

from ..test_base import TestBase

from cdspy.utils import ArrayList


class TestArrayList(TestBase):
    def test_creation(self) -> None:
        al = ArrayList[int]()
        assert al is not None
        assert not al
        assert len(al) == 0

