"""Persistence for crosswalk entries — load, save, diff."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import Literal

CrosswalkStatus = Literal["auto", "confirmed", "rejected", "manual"]


@dataclass
class CrosswalkEntry:
    """A single mapping between a DWH column and an O3 attribute."""

    dwh_table: str
    dwh_column: str
    model_name: str | None
    model_alias: str | None
    model_expr: str | None
    o3_key_element: str
    o3_attribute: str
    confidence: float
    status: CrosswalkStatus

    def __post_init__(self):
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"confidence must be in [0.0, 1.0], got {self.confidence}"
            )

    @property
    def is_active(self) -> bool:
        return self.status in ("auto", "confirmed", "manual")

    @property
    def key(self) -> tuple[str, str, str, str]:
        """Unique identity: (dwh_table, dwh_column, o3_key_element, o3_attribute)."""
        return (self.dwh_table, self.dwh_column, self.o3_key_element, self.o3_attribute)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> CrosswalkEntry:
        return cls(
            dwh_table=data["dwh_table"],
            dwh_column=data["dwh_column"],
            model_name=data.get("model_name"),
            model_alias=data.get("model_alias"),
            model_expr=data.get("model_expr"),
            o3_key_element=data["o3_key_element"],
            o3_attribute=data["o3_attribute"],
            confidence=data["confidence"],
            status=data["status"],
        )


@dataclass
class MappingDiff:
    """Differences between two crosswalk versions."""

    added: list[CrosswalkEntry]
    removed: list[CrosswalkEntry]
    changed: list[tuple[CrosswalkEntry, CrosswalkEntry]]  # (old, new)


class MappingStore:
    """JSON persistence for crosswalk entries."""

    def save(self, entries: list[CrosswalkEntry], path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump([e.to_dict() for e in entries], f, indent=2)

    def load(self, path: str) -> list[CrosswalkEntry]:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return [CrosswalkEntry.from_dict(d) for d in data]

    def diff(
        self, old: list[CrosswalkEntry], new: list[CrosswalkEntry]
    ) -> MappingDiff:
        old_by_key = {e.key: e for e in old}
        new_by_key = {e.key: e for e in new}

        added = [e for k, e in new_by_key.items() if k not in old_by_key]
        removed = [e for k, e in old_by_key.items() if k not in new_by_key]
        changed = [
            (old_by_key[k], new_by_key[k])
            for k in old_by_key.keys() & new_by_key.keys()
            if old_by_key[k] != new_by_key[k]
        ]

        return MappingDiff(added=added, removed=removed, changed=changed)


if __name__ == "__main__":
    pass
