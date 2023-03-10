from __future__ import annotations

from collections.abc import Collection
from typing import Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from . import TableContext


class Tag:
    @staticmethod
    def normalize_label(label: str) -> str:
        if not label.strip():
            return ""
        return " ".join(label.strip().lower().split())

    @staticmethod
    def as_labels(tags: Optional[Collection[Tag]]) -> Collection[str]:
        if not tags:
            return []
        return sorted({t.label for t in tags if isinstance(t, Tag)})

    @staticmethod
    def as_tags(labels: Collection[str], context: TableContext, create: Optional[bool] = True) -> Set[Tag]:
        if labels:
            tags: Set[Tag] = set()
            for label in labels:
                label = Tag.normalize_label(label)
                if not label:
                    continue
                # if tag not present in context, it will be added if
                # create is True, else, None is returned
                tag = context.to_canonical_tag(label, create)

                # if None is returned, create the tag without adding it to context
                tags.add(tag if tag else Tag(label))
            return tags
        return set()

    __slots__ = ["_label"]

    def __init__(self, label: str) -> None:
        self._label = Tag.normalize_label(label)

    def __repr__(self) -> str:
        return f"Tag('{self._label}')"

    def __str__(self) -> str:
        return f"[Tag: '{self._label}']"

    def __hash__(self) -> int:
        return hash(self._label)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Tag):
            return False
        return self._label == other._label

    def __lt__(self, other: Tag) -> bool:
        if not isinstance(other, Tag):
            raise NotImplementedError
        return self._label < other._label

    def __gt__(self, other: Tag) -> bool:
        if not isinstance(other, Tag):
            raise NotImplementedError
        return self._label > other._label

    @property
    def label(self) -> str:
        return self._label
