from __future__ import annotations

from typing import Any


class TestBase:
    def init(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()


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
