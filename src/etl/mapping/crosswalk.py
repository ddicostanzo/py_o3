"""Orchestrates crosswalk generation: auto-suggest DWH→O3 mappings, merge with curated."""

from __future__ import annotations

from etl.manifest import SemanticManifest
from etl.registry import ModelRegistry
from etl.mapping.match_engine import MatchEngine
from etl.mapping.mapping_store import CrosswalkEntry, MappingStore


class Crosswalk:
    """Generates and manages DWH→O3 crosswalk mappings."""

    def __init__(
        self,
        manifest: SemanticManifest,
        registry: ModelRegistry,
        o3_model,
        match_engine: MatchEngine | None = None,
        min_confidence: float = 0.5,
    ):
        self.__manifest = manifest
        self.__registry = registry
        self.__o3_model = o3_model
        self.__engine = match_engine or MatchEngine()
        self.__min_confidence = min_confidence
        self.__deny_list = set(
            getattr(registry.field_policy_defaults, "deny_list", [])
        )

    def generate_suggestions(self) -> list[CrosswalkEntry]:
        """Auto-suggest crosswalk entries by scoring all DWH columns against O3 attributes."""
        suggestions: list[CrosswalkEntry] = []

        # Score DWH table columns against O3 attributes
        for table in self.__manifest.tables.values():
            for col in table.columns:
                if col.name in self.__deny_list:
                    continue
                self.__score_against_o3(
                    suggestions,
                    dwh_table=table.full_name,
                    dwh_column=col.name,
                    dwh_type=col.data_type,
                    dwh_context=table.name,
                    model_name=None,
                    model_alias=None,
                    model_expr=None,
                )

        # Score conceptual model selects against O3 attributes
        for model in self.__manifest.models:
            for select in model.selects:
                if select.alias in self.__deny_list:
                    continue
                self.__score_against_o3(
                    suggestions,
                    dwh_table=model.base_table or "",
                    dwh_column=select.alias,
                    dwh_type=select.data_type,
                    dwh_context=model.name,
                    model_name=model.name,
                    model_alias=select.alias,
                    model_expr=select.expr,
                )

        # Deduplicate: keep highest-scoring entry per (dwh_table, dwh_column, o3_key_element, o3_attribute)
        best: dict[tuple, CrosswalkEntry] = {}
        for entry in suggestions:
            key = entry.key
            if key not in best or entry.confidence > best[key].confidence:
                best[key] = entry

        result = sorted(best.values(), key=lambda e: e.confidence, reverse=True)
        return result

    def __score_against_o3(
        self,
        suggestions: list[CrosswalkEntry],
        dwh_table: str,
        dwh_column: str,
        dwh_type: str,
        dwh_context: str,
        model_name: str | None,
        model_alias: str | None,
        model_expr: str | None,
    ) -> None:
        """Score a single DWH column against all O3 attributes."""
        for ke_name, key_element in self.__o3_model.key_elements.items():
            for attr in key_element.list_attributes:
                candidate = self.__engine.score(
                    dwh_name=dwh_column,
                    dwh_type=dwh_type,
                    o3_name=attr.value_name,
                    o3_type=attr.value_data_type,
                    dwh_context=dwh_context,
                    o3_context=ke_name,
                )
                if candidate.score >= self.__min_confidence:
                    suggestions.append(
                        CrosswalkEntry(
                            dwh_table=dwh_table,
                            dwh_column=dwh_column,
                            model_name=model_name,
                            model_alias=model_alias,
                            model_expr=model_expr,
                            o3_key_element=ke_name,
                            o3_attribute=attr.value_name,
                            confidence=candidate.score,
                            status="auto",
                        )
                    )

    def load_curated(self, path: str) -> list[CrosswalkEntry]:
        return MappingStore().load(path)

    def save_curated(self, entries: list[CrosswalkEntry], path: str) -> None:
        MappingStore().save(entries, path)

    def merge(
        self,
        suggestions: list[CrosswalkEntry],
        curated: list[CrosswalkEntry],
    ) -> list[CrosswalkEntry]:
        """Merge suggestions with curated entries. Curated decisions take precedence."""
        curated_by_key = {e.key: e for e in curated}
        merged: dict[tuple, CrosswalkEntry] = {}

        for entry in suggestions:
            key = entry.key
            if key in curated_by_key:
                merged[key] = curated_by_key[key]
            else:
                merged[key] = entry

        # Include curated entries not in suggestions (manual additions)
        for key, entry in curated_by_key.items():
            if key not in merged:
                merged[key] = entry

        return sorted(merged.values(), key=lambda e: e.confidence, reverse=True)


if __name__ == "__main__":
    pass
