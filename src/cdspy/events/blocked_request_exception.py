from __future__ import annotations

from typing import Any


class BlockedRequestException(Exception):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args)
