from typing import Optional


class TableError(RuntimeError):
    def __init__(self, messaage: Optional[str] = None) -> None:
        super().__init__(messaage)
