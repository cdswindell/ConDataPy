from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import Optional, TYPE_CHECKING

from ordered_set import OrderedSet

from . import Property
from . import BaseElement

if TYPE_CHECKING:
    from . import TableContext
    from ..mixins import Derivable


class TableElement(BaseElement, ABC):
    @property
    @abstractmethod
    def table(self) -> BaseElement:
        pass

    @property
    @abstractmethod
    def table_context(self) -> TableContext:
        pass

    @abstractmethod
    def fill(self, o: Optional[object]) -> bool:
        pass

    @abstractmethod
    def clear(self) -> bool:
        pass

    @property
    @abstractmethod
    def num_cells(self) -> int:
        pass

    @property
    @abstractmethod
    def is_pendings(self) -> bool:
        pass

    @property
    @abstractmethod
    def is_label_indexed(self) -> bool:
        pass

    @property
    @abstractmethod
    def affects(self) -> OrderedSet[Derivable]:
        pass

    @property
    @abstractmethod
    def derived_elements(self) -> Collection[Derivable]:
        pass

    @abstractmethod
    def _delete(self, compress: Optional[bool] = True) -> None:
        pass

    @abstractmethod
    def _register_affects(self, d: Derivable) -> None:
        pass

    @abstractmethod
    def _deregister_affects(self, d: Derivable) -> None:
        pass

    def __init__(self, te: TableElement) -> None:
        super().__init__()
        self._clear_property(Property.Label)
        self._clear_property(Property.Description)
        self.is_enforce_datatype = False

    def delete(self) -> None:
        self._delete(True)
