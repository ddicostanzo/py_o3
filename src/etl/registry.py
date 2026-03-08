# src/etl/registry.py
"""Loader for model_registry.json — entry points, join/time/field policies, query safety."""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass
class DateBasis:
    """Maps human-readable date basis names to DWH date key columns."""

    enum: list[str]
    map: dict[str, str]
    default: str

    @classmethod
    def from_dict(cls, data: dict) -> DateBasis:
        return cls(
            enum=data["enum"],
            map=data["map"],
            default=data["default"],
        )

    def resolve(self, basis: str) -> str:
        """Return the DWH date key column for a given basis name."""
        if basis not in self.map:
            raise ValueError(
                f"invalid date basis '{basis}'; valid options: {self.enum}"
            )
        return self.map[basis]


@dataclass
class TimePolicy:
    """Controls date filtering for an entry point or model."""

    default_date_key: str | None
    date_basis: DateBasis | None = None
    default_lookback_days: int | None = None
    date_key_candidates: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> TimePolicy:
        date_basis_data = data.get("dateBasis")
        return cls(
            default_date_key=data.get("defaultDateKey"),
            date_basis=DateBasis.from_dict(date_basis_data)
            if date_basis_data
            else None,
            default_lookback_days=data.get("defaultLookbackDays"),
            date_key_candidates=data.get("dateKeyCandidates", []),
        )


@dataclass
class JoinSpec:
    """One allowed dimension join: fact.from_column → dimension.to_column."""

    table: str
    from_column: str
    to_column: str

    @classmethod
    def from_dict(cls, data: dict) -> JoinSpec:
        return cls(
            table=data["table"],
            from_column=data["fromColumn"],
            to_column=data["toColumn"],
        )


@dataclass
class JoinPolicy:
    """Controls which dimension joins are allowed for a model."""

    mode: str
    allowed_dimension_joins: list[JoinSpec]

    @classmethod
    def from_dict(cls, data: dict) -> JoinPolicy:
        return cls(
            mode=data["mode"],
            allowed_dimension_joins=[
                JoinSpec.from_dict(j) for j in data.get("allowedDimensionJoins", [])
            ],
        )


@dataclass
class FieldPolicy:
    """Deny list of columns that must never appear in queries."""

    deny_list: list[str]

    @classmethod
    def from_dict(cls, data: dict) -> FieldPolicy:
        return cls(deny_list=data.get("denyList", []))


@dataclass
class ModelConfig:
    """Configuration for a single conceptual model."""

    base_table: str | None
    join_policy: JoinPolicy
    time_policy: TimePolicy
    field_policy: FieldPolicy

    @classmethod
    def from_dict(cls, data: dict) -> ModelConfig:
        return cls(
            base_table=data.get("baseTable"),
            join_policy=JoinPolicy.from_dict(data["joinPolicy"]),
            time_policy=TimePolicy.from_dict(data["timePolicy"]),
            field_policy=FieldPolicy.from_dict(data["fieldPolicy"]),
        )


@dataclass
class EntryPoint:
    """A named entry point into the DWH (e.g., billing, scheduling)."""

    base_table: str
    preferred_conceptual_model: str
    time_policy: TimePolicy

    @classmethod
    def from_dict(cls, data: dict) -> EntryPoint:
        return cls(
            base_table=data["baseTable"],
            preferred_conceptual_model=data["preferredConceptualModel"],
            time_policy=TimePolicy.from_dict(data["timePolicy"]),
        )


@dataclass
class DateRangePolicy:
    """Global date range interpretation rules."""

    default_mode: str
    supported_modes: list[str]

    @classmethod
    def from_dict(cls, data: dict) -> DateRangePolicy:
        return cls(
            default_mode=data["defaultMode"],
            supported_modes=data["supportedModes"],
        )


@dataclass
class QuerySafety:
    """Global safety constraints for generated queries."""

    select_only: bool
    default_row_limit: int
    max_row_limit: int
    require_date_filter_for_tables: list[str]
    cross_fact_joins: str

    @classmethod
    def from_dict(cls, data: dict) -> QuerySafety:
        return cls(
            select_only=data["selectOnly"],
            default_row_limit=data["defaultRowLimit"],
            max_row_limit=data["maxRowLimit"],
            require_date_filter_for_tables=data.get(
                "requireDateFilterForTables", []
            ),
            cross_fact_joins=data.get("crossFactJoins", "disallow_unless_bridge"),
        )


@dataclass
class GlobalPolicy:
    """Global policies: timezone, date ranges, query safety."""

    timezone: str
    date_range: DateRangePolicy
    query_safety: QuerySafety

    @classmethod
    def from_dict(cls, data: dict) -> GlobalPolicy:
        return cls(
            timezone=data["timezone"],
            date_range=DateRangePolicy.from_dict(data["dateRange"]),
            query_safety=QuerySafety.from_dict(data["querySafety"]),
        )


@dataclass
class ModelRegistry:
    """Top-level container for the model registry."""

    entry_points: dict[str, EntryPoint]
    models: dict[str, ModelConfig]
    global_policy: GlobalPolicy
    field_policy_defaults: FieldPolicy

    @classmethod
    def from_dict(cls, data: dict) -> ModelRegistry:
        return cls(
            entry_points={
                k: EntryPoint.from_dict(v)
                for k, v in data.get("entryPoints", {}).items()
            },
            models={
                k: ModelConfig.from_dict(v)
                for k, v in data.get("models", {}).items()
            },
            global_policy=GlobalPolicy.from_dict(data["globalPolicy"]),
            field_policy_defaults=FieldPolicy.from_dict(
                data.get("fieldPolicyDefaults", {"denyList": []})
            ),
        )


def load_model_registry(path: str) -> ModelRegistry:
    """Load a ModelRegistry from a JSON file path."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return ModelRegistry.from_dict(data)


if __name__ == "__main__":
    pass
