from __future__ import annotations

import threading

from typing import Any


# Singleton CLass
class ThreadLocalStorage:
    __instance: ThreadLocalStorage | None = None
    __thread_local = None  # type: ignore

    def __new__(cls, *args: Any, **kwargs: Any) -> ThreadLocalStorage:
        if cls.__instance is None:
            cls.__instance = super(ThreadLocalStorage, cls).__new__(cls)
            cls.__thread_local = threading.local()
        return cls.__instance

    @classmethod
    def put(cls, key: str, value: Any) -> None:
        cls.__thread_local.__setattr__(key, value)

    @classmethod
    def get(cls, key: str) -> Any:
        return cls.__thread_local.__getattribute__(key)

    def __contains__(self, key: str) -> bool:
        return key in ThreadLocalStorage.__thread_local  # type: ignore
