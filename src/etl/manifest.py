# src/etl/manifest.py
"""Loader for semantic manifest files — DWH tables, conceptual models, select expressions."""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass
class Column:
    """A column in a DWH table."""

    name: str
    data_type: str
    nullable: bool
    is_primary_key: bool
    is_foreign_key: bool

    @classmethod
    def from_dict(cls, data: dict) -> Column:
        return cls(
            name=data["name"],
            data_type=data["dataType"],
            nullable=data["nullable"],
            is_primary_key=data["isPrimaryKey"],
            is_foreign_key=data["isForeignKey"],
        )


@dataclass
class ForeignKey:
    """A foreign key relationship in the DWH schema."""

    from_column: str
    to_table: str
    to_column: str

    @classmethod
    def from_dict(cls, data: dict) -> ForeignKey:
        return cls(
            from_column=data["fromColumn"],
            to_table=data["toTable"],
            to_column=data["toColumn"],
        )


@dataclass
class DWHTable:
    """A table in the Varian ARIA Data Warehouse."""

    schema: str
    name: str
    full_name: str
    type: str
    primary_key: list[str]
    columns: list[Column]
    foreign_keys: list[ForeignKey]

    @classmethod
    def from_dict(cls, full_name: str, data: dict) -> DWHTable:
        return cls(
            schema=data["schema"],
            name=data["name"],
            full_name=full_name,
            type=data["type"],
            primary_key=data.get("primaryKey", []),
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
            foreign_keys=[
                ForeignKey.from_dict(fk) for fk in data.get("foreignKeys", [])
            ],
        )

    @property
    def columns_by_name(self) -> dict[str, Column]:
        return {c.name: c for c in self.columns}


@dataclass
class ModelSelect:
    """A SELECT column in a conceptual model — alias, source, expression, type."""

    alias: str
    from_table: str
    expr: str
    data_type: str
    tags: str

    @classmethod
    def from_dict(cls, data: dict) -> ModelSelect:
        return cls(
            alias=data["as"],
            from_table=data["fromTable"],
            expr=data["expr"],
            data_type=data["dataType"],
            tags=data.get("tags", ""),
        )


@dataclass
class ConceptualModel:
    """A named conceptual model with its SELECT columns and join definitions."""

    name: str
    base_table: str | None
    tables_referenced: list[str]
    joins: list[dict]
    selects: list[ModelSelect]

    @classmethod
    def from_dict(cls, data: dict) -> ConceptualModel:
        return cls(
            name=data["name"],
            base_table=data.get("baseTable"),
            tables_referenced=data.get("tablesReferenced", []),
            joins=data.get("joins", []),
            selects=[ModelSelect.from_dict(s) for s in data.get("select", [])],
        )


@dataclass
class SemanticManifest:
    """Combined semantic manifest: DWH tables + conceptual models."""

    tables: dict[str, DWHTable]
    models: list[ConceptualModel]
    summary: dict

    @property
    def models_by_name(self) -> dict[str, ConceptualModel]:
        return {m.name: m for m in self.models}


def load_semantic_manifest(
    schema_path: str, models_path: str
) -> SemanticManifest:
    """Load a SemanticManifest from the two manifest JSON files."""
    with open(schema_path, encoding="utf-8") as f:
        schema_data = json.load(f)

    with open(models_path, encoding="utf-8") as f:
        models_data = json.load(f)

    tables = {
        full_name: DWHTable.from_dict(full_name, table_data)
        for full_name, table_data in schema_data.get("tables", {}).items()
    }

    models = [
        ConceptualModel.from_dict(m) for m in models_data.get("models", [])
    ]

    summary = models_data.get("summary", schema_data.get("summary", {}))

    return SemanticManifest(tables=tables, models=models, summary=summary)


if __name__ == "__main__":
    pass
