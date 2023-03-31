from __future__ import annotations

from typing import Any, Final, TYPE_CHECKING

if TYPE_CHECKING:
    from cdspy.elements import Table

PROPERTY_ABC: Final = "Property ABC"
PROPERTY_DEF: Final = "Property DEF"


class TestBase:
    @staticmethod
    def add_test_rows(t: Table) -> None:
        for x in range(1, 21):
            r = t.add_row()
            r.label = f"Row {x} Label"
            r.description = f"Row {x} Description"
            r.set_property(PROPERTY_ABC, f"Row {x} {PROPERTY_ABC}")
            r.set_property(PROPERTY_DEF, f"Row {x} {PROPERTY_DEF}")

    @staticmethod
    def add_test_columns(t: Table) -> None:
        for x in range(1, 21):
            c = t.add_column()
            c.label = f"Column {x} Label"
            c.description = f"Column {x} Description"
            c.set_property(PROPERTY_ABC, f"Column {x} {PROPERTY_ABC}")
            c.set_property(PROPERTY_DEF, f"Column {x} {PROPERTY_DEF}")

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
