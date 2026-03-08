"""Generates SELECT SQL from DWH models using crosswalk mappings and registry policies."""

from __future__ import annotations

from dataclasses import dataclass, field

from etl.mapping.mapping_store import CrosswalkEntry
from etl.manifest import SemanticManifest
from etl.registry import ModelRegistry, JoinSpec


@dataclass
class ExtractQuery:
    """A generated SELECT query for one entry point."""

    entry_point: str
    model_name: str
    base_table: str
    sql: str
    date_key: str
    joins: list[JoinSpec]
    columns_mapped: list[CrosswalkEntry]


class Extractor:
    """Generates SELECT SQL from DWH models guided by crosswalk entries."""

    def __init__(
        self,
        crosswalk: list[CrosswalkEntry],
        manifest: SemanticManifest,
        registry: ModelRegistry,
    ):
        self.__crosswalk = crosswalk
        self.__manifest = manifest
        self.__registry = registry

    def generate_query(
        self,
        entry_point: str,
        date_basis: str | None = None,
        lookback_days: int | None = None,
    ) -> ExtractQuery:
        """Generate a SELECT query for a given entry point."""
        if entry_point not in self.__registry.entry_points:
            raise ValueError(
                f"Unknown entry point '{entry_point}'; "
                f"valid: {list(self.__registry.entry_points.keys())}"
            )

        ep = self.__registry.entry_points[entry_point]
        model_name = ep.preferred_conceptual_model
        model_config = self.__registry.models.get(model_name)
        base_table = ep.base_table

        # Resolve date key
        time_policy = ep.time_policy
        if date_basis and time_policy.date_basis:
            date_key = time_policy.date_basis.resolve(date_basis)
        else:
            date_key = time_policy.default_date_key or ""

        if lookback_days is not None:
            lookback = lookback_days
        elif time_policy.default_lookback_days is not None:
            lookback = time_policy.default_lookback_days
        else:
            lookback = 90

        # Collect deny list
        deny = set(self.__registry.field_policy_defaults.deny_list)
        if model_config:
            deny |= set(model_config.field_policy.deny_list)

        # Filter crosswalk entries for this entry point's base table
        relevant = [
            e
            for e in self.__crosswalk
            if e.is_active
            and e.dwh_column not in deny
            and (e.model_alias or e.dwh_column) not in deny
        ]

        # Filter to entries related to this model or base table
        model_entries = [
            e for e in relevant
            if e.model_name == model_name or e.dwh_table == base_table
        ]

        if not model_entries:
            raise ValueError(
                f"No crosswalk entries match entry point '{entry_point}' "
                f"(model='{model_name}', base_table='{base_table}'). "
                f"Check the crosswalk or registry configuration."
            )

        # Determine which dimension joins are needed
        needed_joins: list[JoinSpec] = []
        if model_config:
            joined_tables = set()
            for entry in model_entries:
                for join in model_config.join_policy.allowed_dimension_joins:
                    if (
                        join.from_column == entry.dwh_column
                        and join.table not in joined_tables
                    ):
                        needed_joins.append(join)
                        joined_tables.add(join.table)

        # Build SELECT columns
        select_columns = []
        for entry in model_entries:
            alias = entry.model_alias or entry.dwh_column
            if entry.model_expr and entry.model_expr != entry.dwh_column:
                select_columns.append(f"  {entry.model_expr} AS [{alias}]")
            else:
                select_columns.append(f"  base.[{entry.dwh_column}]")

        # Build SQL
        row_limit = self.__registry.global_policy.query_safety.default_row_limit
        select_clause = f"SELECT TOP {row_limit}\n" + ",\n".join(select_columns)
        from_clause = f"FROM {base_table} AS base"

        join_clauses = []
        for join in needed_joins:
            join_clauses.append(
                f"LEFT JOIN {join.table} AS [{join.table.split('.')[-1]}]\n"
                f"  ON base.[{join.from_column}] = [{join.table.split('.')[-1]}].[{join.to_column}]"
            )

        # Date filter
        requires_date = (
            base_table
            in self.__registry.global_policy.query_safety.require_date_filter_for_tables
        )
        where_clause = ""
        if date_key and requires_date:
            where_clause = (
                f"WHERE base.[{date_key}] >= "
                f"(SELECT DimDateID FROM DWH.DimDate WHERE FullDate = CAST(DATEADD(DAY, -{lookback}, GETDATE()) AS DATE))"
            )

        parts = [select_clause, from_clause]
        if join_clauses:
            parts.extend(join_clauses)
        if where_clause:
            parts.append(where_clause)

        sql = "\n".join(parts)

        return ExtractQuery(
            entry_point=entry_point,
            model_name=model_name,
            base_table=base_table,
            sql=sql,
            date_key=date_key,
            joins=needed_joins,
            columns_mapped=model_entries,
        )

    def generate_all_queries(
        self,
        date_basis: str | None = None,
        lookback_days: int | None = None,
    ) -> list[ExtractQuery]:
        """Generate extract queries for all entry points."""
        results = []
        errors = []
        for ep in self.__registry.entry_points:
            try:
                results.append(self.generate_query(ep, date_basis, lookback_days))
            except Exception as e:
                errors.append(
                    f"Entry point '{ep}': {type(e).__name__}: {e}"
                )

        if errors:
            raise RuntimeError(
                f"{len(errors)} entry point(s) failed during query generation:\n"
                + "\n".join(errors)
            )

        return results


if __name__ == "__main__":
    pass
