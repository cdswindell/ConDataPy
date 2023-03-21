from __future__ import annotations

from ..exceptions import InvalidException
from ..exceptions import InvalidParentException

from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import cast, Optional, TYPE_CHECKING

from . import Property
from . import BaseElement
from . import TableContext

if TYPE_CHECKING:
    from . import Tag
    from . import Table

    from ..mixins import Derivable


class TableElement(BaseElement, ABC):
    @property
    @abstractmethod
    def table(self) -> Table:
        pass

    @property
    @abstractmethod
    def table_context(self) -> TableContext:
        pass

    @abstractmethod
    def _delete(self, compress: bool = True) -> None:
        pass

    @abstractmethod
    def fill(self, o: Optional[object]) -> None:
        pass

    @abstractmethod
    def clear(self) -> None:
        pass

    @abstractmethod
    def _register_affects(self, d: Derivable) -> None:
        pass

    @abstractmethod
    def _deregister_affects(self, d: Derivable) -> None:
        pass

    @property
    @abstractmethod
    def num_cells(self) -> int:
        pass

    @property
    @abstractmethod
    def num_groups(self) -> int:
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
    def affects(self) -> Collection[Derivable]:
        pass

    @property
    @abstractmethod
    def derived_elements(self) -> Collection[Derivable]:
        pass

    def __init__(self, te: Optional[TableElement] = None) -> None:
        super().__init__()

    def delete(self) -> None:
        self._delete(True)

    @property
    def _tags(self) -> set[Tag]:
        """
        Protected method to retrieve Tags collection from element properties
        """
        from . import Tag

        return cast(set[Tag], self.get_property(Property.Tags))

    @property
    def tags(self) -> Collection[str]:
        from . import Tag

        with self.lock:
            return Tag.as_labels(self._tags)

    @tags.setter
    def tags(self, *tags: str) -> None:
        from . import Tag

        print(f"{tags} {tags[0]} {type(tags)} {type(tags[0])} {self._normalize(tags)}")
        with self.lock:
            if tags:
                new_tags = Tag.as_tags(self._normalize(tags), self.table_context)
                self._initialize_property(Property.Tags, new_tags)
            else:
                self._clear_property(Property.Tags)

    def tag(self, *tags: str) -> bool:
        from . import Tag

        if tags:
            tc = self.table_context
            with self.lock:
                new_tags: set[Tag] = Tag.as_tags(tags, tc)
                if new_tags:
                    cur_tags: set[Tag] = self._tags
                    if cur_tags:
                        prev_len = len(cur_tags)
                        cur_tags.update(new_tags)
                        return len(cur_tags) > prev_len
                    else:
                        self._initialize_property(Property.Tags, new_tags)
                        return True
        return False

    def untag(self, *tags: str) -> bool:
        from . import Tag

        with self.lock:
            cur_tags: set[Tag] = self._tags
            if tags and cur_tags:
                un_tags: set[Tag] = Tag.as_tags(tags, self.table_context, False)
                if un_tags and cur_tags & un_tags:
                    self._initialize_property(Property.Tags, cur_tags - un_tags)
                    return True
            return False

    def has_all_tags(self, *tags: str) -> bool:
        from . import Tag

        if tags:
            with self.lock:
                cur_tags = self._tags if self.table else set()
                if cur_tags:
                    query_tags = Tag.as_tags(tags, self.table_context, False)
                    # return True if query_tags is a proper subset of current tags
                    return bool(query_tags) and cur_tags >= query_tags
        return False

    def has_any_tags(self, *tags: str) -> bool:
        from . import Tag

        if tags:
            with self.lock:
                cur_tags = self._tags if self.table else set()
                if cur_tags:
                    query_tags = Tag.as_tags(tags, self.table_context, False)
                    # return True if query_tags is a proper subset of current tags
                    return bool(cur_tags & query_tags)
        return False

    def vet_components(self, te: TableElement) -> None:
        self.vet_element()
        if not self.table:
            raise InvalidException(self, f"{self.element_type.name} Requires a Parent Table")
        self.table.vet_element()
        if te:
            self.vet_element()
            if self.table != te.table:
                raise InvalidParentException(self, te)
