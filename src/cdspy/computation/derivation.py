from __future__ import annotations

from typing import Final, Optional, List, TYPE_CHECKING
import uuid

from ..elements import TableElement

from ..utils.atomic_integer import AtomicInteger

if TYPE_CHECKING:
    from ..elements import Cell


class _DerivationContext:
    __slots__: List[str] = ["_cached_any", "_is_recalculate_affected", "_pendings"]

    def __init__(self) -> None:
        self._cached_any = False
        self._is_recalculate_affected = True

    @property
    def is_recalculate_affected(self) -> bool:
        return self._is_recalculate_affected

    @is_recalculate_affected.setter
    def is_recalculate_affected(self, recalc: bool) -> None:
        self._is_recalculate_affected = bool(recalc)

    @property
    def is_any_cached(self) -> bool:
        return self._cached_any

    @is_any_cached.setter
    def is_any_cached(self, cached: bool) -> None:
        self._cached_any = bool(cached)


class Derivation:
    _ELEMENT_IDENT_GENERATOR: Final = AtomicInteger(1000)

    __slots__: List[str] = ["_ident"]

    def __init__(self) -> None:
        super().__init__()
        self._ident = Derivation._ELEMENT_IDENT_GENERATOR.inc()

    @property
    def ident(self) -> int:
        return self._ident

    def recalculate_target(self) -> None:
        pass

    def lookup_remote_uuid_by_cell(self, cell: Cell) -> uuid.UUID | None:
        return None


def recalculate_affected(te: TableElement, dc: Optional[_DerivationContext] = None) -> None:
    if dc is None:
        dc = _DerivationContext()
    dc.is_recalculate_affected = False
