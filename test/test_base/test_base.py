from __future__ import annotations

from typing import Any


class TestBase:
    def init(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
