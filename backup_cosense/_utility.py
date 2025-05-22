from __future__ import annotations

import dataclasses
import datetime
import itertools
import pathlib
from typing import Optional, Self

from .exceptions import CommitTargetError


@dataclasses.dataclass
class CommitTarget:
    added: set[pathlib.Path] = dataclasses.field(default_factory=set)
    updated: set[pathlib.Path] = dataclasses.field(default_factory=set)
    deleted: set[pathlib.Path] = dataclasses.field(default_factory=set)

    def __post_init__(self) -> None:
        self.normalize()
        self.validate()

    def normalize(self) -> None:
        self.added = {path.resolve() for path in self.added}
        self.updated = {path.resolve() for path in self.updated}
        self.deleted = {path.resolve() for path in self.deleted}

    def validate(self) -> None:
        errors: list[str] = []
        # check intersection
        for set1, set2 in itertools.combinations(["added", "updated", "deleted"], 2):
            for path in getattr(self, set1) & getattr(self, set2):
                errors.append(f'"{path}" exists in both "{set1}" and "{set2}"')
        # raise error
        if errors:
            raise CommitTargetError("".join(errors))

    def update(self, other: Self) -> Self:
        self.added |= other.added
        self.updated |= other.updated
        self.deleted |= other.deleted
        self.validate()
        return self

    def is_empty(self) -> bool:
        return not (self.added or self.updated or self.deleted)


def format_timestamp(timestamp: Optional[int]) -> str:
    if timestamp is None:
        return "None"
    return f"{datetime.datetime.fromtimestamp(timestamp)} ({timestamp})"
