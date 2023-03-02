from __future__ import annotations


class Tag:
    @staticmethod
    def as_strings(tags: set(Tag)) -> list[str]:
        return sorted([t.label for t in tags])

    def __init__(self, label: str) -> None:
        self._label = " ".join(label.strip().lower().split())

    def __repr__(self) -> str:
        return f"Tag('{self._label}')"

    def __str__(self) -> str:
        return f"Tag: '{self._label}'"

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


class Category(Tag):
    def __repr__(self) -> str:
        return f"Category('{self._label}')"

    def __str__(self) -> str:
        return f"Category: '{self._label}'"
