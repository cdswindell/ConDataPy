# mypy: ignore-errors
from __future__ import annotations

import pytest

from ..test_base import TestBase

from cdspy.utils import ArrayList
from cdspy.utils.array_list import _DEFAULT_CAPACITY_INCREMENT


# noinspection PyTypeChecker
class TestArrayList(TestBase):
    def test_creation(self) -> None:
        # basic state
        al = ArrayList[int]()
        assert al is not None
        assert not al
        assert len(al) == 0
        assert al.capacity == 0
        assert al.capacity_increment == _DEFAULT_CAPACITY_INCREMENT

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
        assert al.capacity == _DEFAULT_CAPACITY_INCREMENT

    def test_initialize_from_list(self) -> None:
        al = ArrayList[int](range(0, 100))
        assert al
        assert len(al) == len(range(0, 100))
        for i in range(0, 100):
            assert al[i] == i
            assert al.count(i) == 1

        al.reverse()
        for i in range(0, 100):
            assert al[i] == 99 - i
            assert al.count(i) == 1

        al.reverse()
        for i in range(0, 100):
            assert al[i] == i
            assert al.count(i) == 1

        al.extend(range(100, 200))
        assert len(al) == len(range(0, 200))
        assert al[0:10:2] == [0, 2, 4, 6, 8]
        assert al[10:101:10] == [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

        for i in range(0, 200):
            assert al[i] == i
            assert al.count(i) == 1

        al.reverse()
        for i in range(0, 200):
            assert al[i] == 199 - i
            assert al.count(i) == 1

        al.reverse()
        al.append(200)
        assert len(al) == len(range(0, 200)) + 1

    def test_valid_args(self) -> None:
        al = ArrayList(None, 17)
        assert al is not None
        assert len(al) == 0
        assert al.capacity == 17
        assert al.capacity_increment == _DEFAULT_CAPACITY_INCREMENT

        al = ArrayList(initial_capacity=22)
        assert al is not None
        assert len(al) == 0
        assert al.capacity == 22
        assert al.capacity_increment == _DEFAULT_CAPACITY_INCREMENT

        al = ArrayList(None, None, capacity_increment=17)
        assert al is not None
        assert len(al) == 0
        assert al.capacity == 0
        assert al.capacity_increment == 17

        al = ArrayList(capacity_increment=22)
        assert al is not None
        assert len(al) == 0
        assert al.capacity == 0
        assert al.capacity_increment == 22

        al = ArrayList(None, 22, 17)
        assert al is not None
        assert len(al) == 0
        assert al.capacity == 22
        assert al.capacity_increment == 17

        al = ArrayList(initial_capacity=22, capacity_increment=17)
        assert al is not None
        assert len(al) == 0
        assert al.capacity == 22
        assert al.capacity_increment == 17

    def test_invalid_args(self) -> None:
        with pytest.raises(NotImplementedError, match="Can not create ArrayList from 'object'"):
            al = ArrayList(object())
        with pytest.raises(NotImplementedError, match="Can not create ArrayList from 'int'"):
            al = ArrayList(12)

        # Test invalid initial_capacity args
        with pytest.raises(ValueError, match="initial_capacity must be >= 0"):
            al = ArrayList(None, object())
        with pytest.raises(ValueError, match="initial_capacity must be >= 0"):
            al = ArrayList(None, -1)
        with pytest.raises(ValueError, match="initial_capacity must be >= 0"):
            al = ArrayList(initial_capacity=object())
        with pytest.raises(ValueError, match="initial_capacity must be >= 0"):
            al = ArrayList(initial_capacity=-1)

        # Test invalid capacity_increment args
        with pytest.raises(ValueError, match="capacity_increment must be >= 0"):
            al = ArrayList(None, None, object())
        with pytest.raises(ValueError, match="capacity_increment must be >= 0"):
            al = ArrayList(None, None, -1)
        with pytest.raises(ValueError, match="capacity_increment must be >= 0"):
            al = ArrayList(capacity_increment=object())
        with pytest.raises(ValueError, match="capacity_increment must be >= 0"):
            al = ArrayList(capacity_increment=-1)

    def test_trim(self) -> None:
        al = ArrayList(range(0, 10), capacity_increment=16)
        assert al is not None
        assert len(al) == 10
        assert al.capacity == 10
        assert al.capacity_increment == 16
        al.ensure_capacity()
        assert al.capacity == 16

        # trim removes all extra capacity, leaving only "active" slots
        al.trim()
        assert len(al) == 10
        assert al.capacity == 10

        al.extend(range(10, 20))
        assert len(al) == 20
        assert al.capacity == 32
        del al[5:15]
        assert len(al) == 10
        assert al.capacity == 32
        assert len(al) == 10

        # trim_to_capacity removes all capacity in excess of the number of
        # active cells rounded up to the nearest multiple of capacity_increment
        al.trim_to_capacity()
        assert al.capacity == 16

        # if capacity_increment is 0, trim_to_capacity just performs a trim
        al = ArrayList(range(0, 20), capacity_increment=0)
        assert len(al) == 20
        assert al.capacity == 20
        assert al.capacity_increment == 0
        al.ensure_capacity()
        assert al.capacity == 20

        del al[5:15]
        assert len(al) == 10
        assert al.capacity == 20
        al.trim_to_capacity()
        assert al.capacity == 10

    def test_iter(self) -> None:
        al = ArrayList[int](range(0, 5))
        assert al
        assert len(al) == 5

        # data structure itself is an iterator; next should directly apply
        assert next(al) == 0
        assert next(al) == 1
        assert next(al) == 2
        assert next(al) == 3
        assert next(al) == 4
        with pytest.raises(StopIteration):
            assert next(al)

        # test via call to iter
        ali = iter(al)
        assert ali
        assert ali == al  # also tests equality
        assert next(al) == 0
        assert next(al) == 1
        assert next(al) == 2
        assert next(al) == 3
        assert next(al) == 4
        with pytest.raises(StopIteration):
            assert next(al)

    def test_extend(self) -> None:
        al = ArrayList[int](range(0, 5))
        assert al
        assert len(al) == 5

        al2 = ArrayList[int](range(5, 10))
        assert al2
        assert len(al2) == 5

        al.extend(al2)
        assert len(al) == 10
        assert al == ArrayList[int](range(0, 10))

        # extend using iterable
        al2.extend(range(10, 15))
        assert len(al2) == 10
        assert al2 == ArrayList[int](range(5, 15))

        # extend using list
        al = ArrayList[int](range(0, 5))
        assert al
        assert len(al) == 5
        al.extend([5, 6, 7, 8, 9])
        assert len(al) == 10
        assert al == ArrayList[int](range(0, 10))

    def test_add(self) -> None:
        al = ArrayList[int](range(0, 5))
        assert al
        assert len(al) == 5

        al2 = ArrayList[int](range(5, 10))
        assert al2
        assert len(al2) == 5

        al3 = al + al2
        assert len(al3) == 10
        assert al3 == ArrayList[int](range(0, 10))
        assert al != al3
        assert al2 != al3

        # add a single item
        al3 = al + 5
        assert len(al3) == 6
        assert al3 == ArrayList[int](range(0, 6))
        assert al != al3

    def test_iadd(self) -> None:
        al = ArrayList[int](range(0, 5))
        assert al
        assert len(al) == 5

        al2 = ArrayList[int](range(5, 10))
        assert al2
        assert len(al2) == 5

        alp = al
        al += al2
        assert len(al) == 10
        assert al == ArrayList[int](range(0, 10))
        assert alp == al
        assert len(alp) == 10
        assert al2 != al

        # add a single item
        al2 += 5
        assert len(al2) == 6
        assert al2 == ArrayList[int]([5, 6, 7, 8, 9, 5])
        assert al2 != al

    def test_mul_rmul_imul(self) -> None:
        al = ArrayList[int](range(0, 5))
        assert al
        assert len(al) == 5

        al2 = al * 3
        assert len(al2) == 15
        assert al2 == ArrayList[int]([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4])
        assert len(al) == 5
        assert al == ArrayList[int](range(0, 5))
        assert al != al2

        al2 = al * -3
        assert len(al2) == 0

        # __rmul__
        al2 = 3 * al
        assert len(al2) == 15
        assert al2 == ArrayList[int]([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4])
        assert len(al) == 5
        assert al == ArrayList[int](range(0, 5))
        assert al != al2

        al2 = -3 * al
        assert len(al2) == 0

        # __imul__
        al *= 3
        assert len(al) == 15
        assert al == ArrayList[int]([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4])

        al = ArrayList[int](range(0, 5))
        al *= -3
        assert len(al) == 0
        assert al == ArrayList[int]()

        al = ArrayList[int](range(0, 5))
        al *= 0
        assert len(al) == 0
        assert al == ArrayList[int]()

    def test_copy(self) -> None:
        from copy import copy
        al = ArrayList[int](range(0, 5))
        assert al
        assert len(al) == 5

        al2 = al.copy()
        assert al2
        assert len(al2) == 5
        assert al2 == al
        assert id(al2) != id(al)

        al3 = copy(al)
        assert al3
        assert len(al3) == 5
        assert al3 == al2 == al
        assert id(al3) != id(al)
