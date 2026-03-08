# ETL Mapping Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a layered ETL framework that auto-suggests DWH→O3 crosswalk mappings, generates data lineage documentation, and produces executable Python+SQL ETL pipelines.

**Architecture:** Three layers (mapping → lineage → pipeline) sharing two foundation loaders (`registry.py`, `manifest.py`). Each layer is independently testable. Crosswalk JSON file is the curated artifact connecting everything.

**Tech Stack:** Python 3.10+ dataclasses, existing O3DataModel/MSSQLConnection, pytest with MagicMock factories, JSON persistence.

**Design Doc:** `docs/plans/2026-03-08-etl-mapping-design.md`

---

### Task 1: Project Scaffolding

**Files:**
- Create: `src/etl/__init__.py`
- Create: `src/etl/mapping/__init__.py`
- Create: `src/etl/lineage/__init__.py`
- Create: `src/etl/pipeline/__init__.py`
- Create: `tests/etl/__init__.py`

**Step 1: Create directory structure**

```bash
mkdir -p src/etl/mapping src/etl/lineage src/etl/pipeline tests/etl
touch src/etl/__init__.py src/etl/mapping/__init__.py src/etl/lineage/__init__.py src/etl/pipeline/__init__.py tests/etl/__init__.py
```

**Step 2: Commit**

```bash
git add src/etl/ tests/etl/
git commit -m "scaffold: create etl module directory structure"
```

---

### Task 2: Model Registry Loader — Dataclasses

**Files:**
- Create: `src/etl/registry.py`
- Create: `tests/etl/test_registry.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_registry.py
import json
import os
import pytest
from etl.registry import (
    DateBasis,
    EntryPoint,
    FieldPolicy,
    JoinPolicy,
    JoinSpec,
    ModelConfig,
    ModelRegistry,
    QuerySafety,
    TimePolicy,
    load_model_registry,
)


REGISTRY_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "src", "Resources", "model_registry.json"
)


def _make_minimal_registry() -> dict:
    """Minimal valid model_registry.json structure."""
    return {
        "version": "1.0",
        "entryPoints": {
            "billing": {
                "baseTable": "DWH.FactActivityBilling",
                "preferredConceptualModel": "dwActivityBillingModel",
                "timePolicy": {
                    "defaultDateKey": "DimDateID_FromDateOfService",
                    "dateBasis": {
                        "enum": ["service"],
                        "map": {"service": "DimDateID_FromDateOfService"},
                        "default": "service",
                    },
                    "defaultLookbackDays": 90,
                },
            }
        },
        "models": {
            "dwActivityBillingModel": {
                "baseTable": "DWH.FactActivityBilling",
                "joinPolicy": {
                    "mode": "facts-first",
                    "allowedDimensionJoins": [
                        {
                            "table": "DWH.DimPatient",
                            "fromColumn": "DimPatientID",
                            "toColumn": "DimPatientID",
                        }
                    ],
                },
                "timePolicy": {
                    "dateKeyCandidates": ["DimDateID_FromDateOfService"],
                    "defaultDateKey": "DimDateID_FromDateOfService",
                    "dateBasis": {
                        "enum": ["service"],
                        "map": {"service": "DimDateID_FromDateOfService"},
                        "default": "service",
                    },
                },
                "fieldPolicy": {"denyList": ["PatientSSN"]},
            }
        },
        "globalPolicy": {
            "timezone": "America/New_York",
            "dateRange": {
                "defaultMode": "inclusive",
                "supportedModes": ["inclusive", "exclusive"],
                "notes": [],
            },
            "querySafety": {
                "selectOnly": True,
                "defaultRowLimit": 1000,
                "maxRowLimit": 100000,
                "requireDateFilterForTables": ["DWH.FactActivityBilling"],
                "crossFactJoins": "disallow_unless_bridge",
                "notes": [],
            },
        },
        "fieldPolicyDefaults": {"denyList": ["PatientSSN"], "notes": []},
    }


class TestDateBasis:
    def test_from_dict(self):
        data = {
            "enum": ["service", "completed"],
            "map": {
                "service": "DimDateID_FromDateOfService",
                "completed": "DimDateID_CompletedDateTime",
            },
            "default": "service",
        }
        db = DateBasis.from_dict(data)
        assert db.enum == ["service", "completed"]
        assert db.map["service"] == "DimDateID_FromDateOfService"
        assert db.default == "service"

    def test_resolve_returns_mapped_key(self):
        db = DateBasis(
            enum=["service"],
            map={"service": "DimDateID_FromDateOfService"},
            default="service",
        )
        assert db.resolve("service") == "DimDateID_FromDateOfService"

    def test_resolve_invalid_raises(self):
        db = DateBasis(enum=["service"], map={"service": "X"}, default="service")
        with pytest.raises(ValueError, match="invalid"):
            db.resolve("nonexistent")


class TestTimePolicy:
    def test_from_entry_point_dict(self):
        data = {
            "defaultDateKey": "DimDateID_FromDateOfService",
            "dateBasis": {
                "enum": ["service"],
                "map": {"service": "DimDateID_FromDateOfService"},
                "default": "service",
            },
            "defaultLookbackDays": 90,
        }
        tp = TimePolicy.from_dict(data)
        assert tp.default_date_key == "DimDateID_FromDateOfService"
        assert tp.default_lookback_days == 90
        assert tp.date_basis is not None
        assert tp.date_basis.default == "service"

    def test_from_model_dict_with_candidates(self):
        data = {
            "dateKeyCandidates": ["DimDateID_FromDateOfService"],
            "defaultDateKey": "DimDateID_FromDateOfService",
            "dateBasis": {
                "enum": ["service"],
                "map": {"service": "DimDateID_FromDateOfService"},
                "default": "service",
            },
        }
        tp = TimePolicy.from_dict(data)
        assert tp.date_key_candidates == ["DimDateID_FromDateOfService"]

    def test_null_date_key(self):
        data = {"dateKeyCandidates": [], "defaultDateKey": None}
        tp = TimePolicy.from_dict(data)
        assert tp.default_date_key is None
        assert tp.date_basis is None


class TestJoinSpec:
    def test_from_dict(self):
        data = {
            "table": "DWH.DimPatient",
            "fromColumn": "DimPatientID",
            "toColumn": "DimPatientID",
        }
        js = JoinSpec.from_dict(data)
        assert js.table == "DWH.DimPatient"
        assert js.from_column == "DimPatientID"
        assert js.to_column == "DimPatientID"


class TestLoadModelRegistry:
    def test_load_from_minimal_dict(self):
        data = _make_minimal_registry()
        registry = ModelRegistry.from_dict(data)

        assert "billing" in registry.entry_points
        ep = registry.entry_points["billing"]
        assert ep.base_table == "DWH.FactActivityBilling"
        assert ep.preferred_conceptual_model == "dwActivityBillingModel"
        assert ep.time_policy.default_lookback_days == 90

        assert "dwActivityBillingModel" in registry.models
        model = registry.models["dwActivityBillingModel"]
        assert model.base_table == "DWH.FactActivityBilling"
        assert model.join_policy.mode == "facts-first"
        assert len(model.join_policy.allowed_dimension_joins) == 1
        assert model.field_policy.deny_list == ["PatientSSN"]

        assert registry.global_policy.query_safety.select_only is True
        assert registry.global_policy.query_safety.default_row_limit == 1000

    def test_load_from_file(self):
        if not os.path.exists(REGISTRY_PATH):
            pytest.skip("model_registry.json not found")
        registry = load_model_registry(REGISTRY_PATH)

        assert len(registry.entry_points) == 4
        assert "billing" in registry.entry_points
        assert "scheduling" in registry.entry_points
        assert "activity" in registry.entry_points
        assert "treatment_history" in registry.entry_points

        assert len(registry.models) == 11
        assert registry.global_policy.query_safety.max_row_limit == 100000

    def test_field_policy_defaults_loaded(self):
        data = _make_minimal_registry()
        registry = ModelRegistry.from_dict(data)
        assert "PatientSSN" in registry.field_policy_defaults.deny_list
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'etl.registry'`

**Step 3: Write implementation**

```python
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
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_registry.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/registry.py tests/etl/test_registry.py
git commit -m "feat: add model registry loader with dataclasses and tests"
```

---

### Task 3: Semantic Manifest Loader — Dataclasses

**Files:**
- Create: `src/etl/manifest.py`
- Create: `tests/etl/test_manifest.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_manifest.py
import json
import os
import pytest
from etl.manifest import (
    Column,
    ConceptualModel,
    DWHTable,
    ForeignKey,
    ModelSelect,
    SemanticManifest,
    load_semantic_manifest,
)


SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "src",
    "Resources",
    "semantic_manifest_from_variandw_schema.json",
)
MODELS_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "src",
    "Resources",
    "semantic_manifest_with_models.json",
)


def _make_column_dict(**overrides) -> dict:
    base = {
        "name": "PatientID",
        "dataType": "int",
        "nullable": False,
        "isPrimaryKey": True,
        "isForeignKey": False,
    }
    base.update(overrides)
    return base


def _make_table_dict(**overrides) -> dict:
    base = {
        "schema": "DWH",
        "name": "DimPatient",
        "type": "dimension",
        "primaryKey": ["DimPatientID"],
        "columns": [_make_column_dict()],
        "foreignKeys": [],
    }
    base.update(overrides)
    return base


def _make_select_dict(**overrides) -> dict:
    base = {
        "as": "PatientId",
        "fromTable": "Patient",
        "expr": "PatientId",
        "dataType": "VDT_PATIENTID",
        "tags": "",
    }
    base.update(overrides)
    return base


class TestColumn:
    def test_from_dict(self):
        col = Column.from_dict(_make_column_dict())
        assert col.name == "PatientID"
        assert col.data_type == "int"
        assert col.nullable is False
        assert col.is_primary_key is True

    def test_nullable_column(self):
        col = Column.from_dict(_make_column_dict(nullable=True, isPrimaryKey=False))
        assert col.nullable is True
        assert col.is_primary_key is False


class TestForeignKey:
    def test_from_dict(self):
        data = {
            "fromColumn": "DimPatientID",
            "toTable": "DWH.DimPatient",
            "toColumn": "DimPatientID",
        }
        fk = ForeignKey.from_dict(data)
        assert fk.from_column == "DimPatientID"
        assert fk.to_table == "DWH.DimPatient"
        assert fk.to_column == "DimPatientID"


class TestDWHTable:
    def test_from_dict(self):
        t = DWHTable.from_dict("DWH.DimPatient", _make_table_dict())
        assert t.schema == "DWH"
        assert t.name == "DimPatient"
        assert t.full_name == "DWH.DimPatient"
        assert t.type == "dimension"
        assert len(t.columns) == 1
        assert t.primary_key == ["DimPatientID"]

    def test_with_foreign_keys(self):
        fks = [
            {
                "fromColumn": "DimPatientID",
                "toTable": "DWH.DimPatient",
                "toColumn": "DimPatientID",
            }
        ]
        t = DWHTable.from_dict(
            "DWH.FactActivityBilling",
            _make_table_dict(
                schema="DWH",
                name="FactActivityBilling",
                type="fact",
                foreignKeys=fks,
            ),
        )
        assert t.type == "fact"
        assert len(t.foreign_keys) == 1

    def test_columns_by_name(self):
        t = DWHTable.from_dict("DWH.DimPatient", _make_table_dict())
        assert "PatientID" in t.columns_by_name
        assert t.columns_by_name["PatientID"].data_type == "int"


class TestModelSelect:
    def test_from_dict(self):
        ms = ModelSelect.from_dict(_make_select_dict())
        assert ms.alias == "PatientId"
        assert ms.from_table == "Patient"
        assert ms.expr == "PatientId"
        assert ms.data_type == "VDT_PATIENTID"

    def test_computed_expression(self):
        ms = ModelSelect.from_dict(
            _make_select_dict(
                **{
                    "as": "ActualGantryRtn",
                    "fromTable": "NA",
                    "expr": "[DWH].[vf_ClinicalScaleConversion]('GRtn', ...)",
                }
            )
        )
        assert ms.alias == "ActualGantryRtn"
        assert ms.from_table == "NA"
        assert "vf_ClinicalScaleConversion" in ms.expr


class TestConceptualModel:
    def test_from_dict(self):
        data = {
            "name": "dwActivityBillingModel",
            "baseTable": "DWH.FactActivityBilling",
            "tablesReferenced": ["DWH.FactActivityBilling", "DWH.DimPatient"],
            "joins": [],
            "select": [_make_select_dict()],
        }
        cm = ConceptualModel.from_dict(data)
        assert cm.name == "dwActivityBillingModel"
        assert cm.base_table == "DWH.FactActivityBilling"
        assert len(cm.selects) == 1
        assert cm.selects[0].alias == "PatientId"


class TestLoadSemanticManifest:
    def test_load_from_files(self):
        if not os.path.exists(SCHEMA_PATH) or not os.path.exists(MODELS_PATH):
            pytest.skip("semantic manifest files not found")
        manifest = load_semantic_manifest(SCHEMA_PATH, MODELS_PATH)

        assert manifest.summary["tableCount"] == 352
        assert len(manifest.tables) == 352
        assert len(manifest.models) == 11

        # Verify fact table
        billing = manifest.tables["DWH.FactActivityBilling"]
        assert billing.type == "fact"
        assert len(billing.foreign_keys) > 0

        # Verify model
        billing_model = next(
            m for m in manifest.models if m.name == "dwActivityBillingModel"
        )
        assert billing_model.base_table == "DWH.FactActivityBilling"
        assert len(billing_model.selects) > 0

    def test_fact_dimension_other_counts(self):
        if not os.path.exists(SCHEMA_PATH):
            pytest.skip("schema manifest not found")
        manifest = load_semantic_manifest(SCHEMA_PATH, MODELS_PATH)
        facts = [t for t in manifest.tables.values() if t.type == "fact"]
        dims = [t for t in manifest.tables.values() if t.type == "dimension"]
        others = [t for t in manifest.tables.values() if t.type == "other"]
        assert len(facts) == 40
        assert len(dims) == 96
        assert len(others) == 216
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_manifest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'etl.manifest'`

**Step 3: Write implementation**

```python
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
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_manifest.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/manifest.py tests/etl/test_manifest.py
git commit -m "feat: add semantic manifest loader with dataclasses and tests"
```

---

### Task 4: Match Engine — Name/Type/Context Scoring

**Files:**
- Create: `src/etl/mapping/match_engine.py`
- Create: `tests/etl/test_match_engine.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_match_engine.py
import pytest
from etl.mapping.match_engine import MatchCandidate, MatchEngine


class TestNameSimilarity:
    def setup_method(self):
        self.engine = MatchEngine()

    def test_exact_match(self):
        score = self.engine._name_similarity("PatientId", "PatientId")
        assert score == 1.0

    def test_case_insensitive(self):
        score = self.engine._name_similarity("patientid", "PatientId")
        assert score == 1.0

    def test_prefix_stripping(self):
        score = self.engine._name_similarity("DimPatientID", "PatientIdentifier")
        assert score > 0.5

    def test_no_overlap(self):
        score = self.engine._name_similarity("GantryRotation", "BillingCode")
        assert score < 0.3

    def test_partial_token_overlap(self):
        score = self.engine._name_similarity("PatientFirstName", "PatientName")
        assert score > 0.5


class TestTypeCompatibility:
    def setup_method(self):
        self.engine = MatchEngine()

    def test_string_types_compatible(self):
        score = self.engine._type_compatibility("varchar", "String")
        assert score > 0.8

    def test_int_types_compatible(self):
        score = self.engine._type_compatibility("int", "Integer")
        assert score > 0.8

    def test_vdt_type_compatible(self):
        score = self.engine._type_compatibility("VDT_PATIENTID", "String")
        assert score > 0.5

    def test_incompatible_types(self):
        score = self.engine._type_compatibility("datetime", "Boolean")
        assert score < 0.3


class TestScore:
    def setup_method(self):
        self.engine = MatchEngine()

    def test_high_confidence_match(self):
        candidate = self.engine.score(
            dwh_name="PatientId",
            dwh_type="VDT_PATIENTID",
            o3_name="PatientIdentifier",
            o3_type="String",
        )
        assert isinstance(candidate, MatchCandidate)
        assert candidate.score >= 0.5
        assert "name" in candidate.signals
        assert "type" in candidate.signals

    def test_low_confidence_mismatch(self):
        candidate = self.engine.score(
            dwh_name="CollRtnOverrideFlag",
            dwh_type="VDT_OVERRIDEFLAG",
            o3_name="PatientIdentifier",
            o3_type="String",
        )
        assert candidate.score < 0.5

    def test_context_bonus(self):
        base = self.engine.score(
            dwh_name="DateOfBirth",
            dwh_type="datetime",
            o3_name="PatientDateOfBirth",
            o3_type="Date",
        )
        boosted = self.engine.score(
            dwh_name="DateOfBirth",
            dwh_type="datetime",
            o3_name="PatientDateOfBirth",
            o3_type="Date",
            dwh_context="Patient",
            o3_context="Patient",
        )
        assert boosted.score >= base.score
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_match_engine.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/mapping/match_engine.py
"""Scoring engine for DWH→O3 column name and type matching."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


# Prefixes stripped before name comparison
_STRIP_PREFIXES = re.compile(
    r"^(Dim|Fact|DimLookupID_|DimDateID_|DimUserID_|DimDoctorID_|ID_?|Lookup)", re.IGNORECASE
)

# Token splitter: camelCase and underscores
_TOKEN_SPLIT = re.compile(r"[A-Z][a-z]+|[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\b)|[A-Z]+|\d+")

# Type compatibility groups
_TYPE_GROUPS: dict[str, set[str]] = {
    "string": {
        "string", "varchar", "nvarchar", "char", "text", "ntext",
        "vdt_patientid", "vdt_id", "vdt_name", "vdt_string",
        "vdt_string1", "vdt_string10", "vdt_string16", "vdt_string30",
        "vdt_string32", "vdt_string64", "vdt_string128", "vdt_string254",
        "vdt_string256", "vdt_string512", "vdt_status16", "vdt_status32",
        "vdt_username", "vdt_phonenumber", "vdt_tablename", "vdt_sex",
        "vdt_collmode", "vdt_energymode", "vdt_scale",
    },
    "integer": {
        "integer", "int", "bigint", "smallint", "tinyint",
        "vdt_int", "vdt_count", "vdt_serialnumber", "vdt_tinyint",
    },
    "decimal": {
        "decimal", "numeric", "float", "real", "money",
        "vdt_float", "vdt_dose", "vdt_doserate", "vdt_angle",
        "vdt_mu", "vdt_energy", "vdt_couchparam", "vdt_collparam",
        "vdt_overrideflag", "vdt_time",
    },
    "date": {
        "date", "datetime", "datetime2", "datetimeoffset", "smalldatetime",
        "vdt_datetime", "vdt_datetimestamp",
    },
    "boolean": {
        "boolean", "bit", "binary",
        "vdt_flag_false_default",
    },
}

# Reverse map: type string → group name
_TYPE_TO_GROUP: dict[str, str] = {}
for group, members in _TYPE_GROUPS.items():
    for member in members:
        _TYPE_TO_GROUP[member] = group


@dataclass
class MatchCandidate:
    """A scored candidate mapping between a DWH source and an O3 attribute."""

    dwh_source: str
    o3_target: str
    score: float
    signals: dict[str, float]


class MatchEngine:
    """Scores potential DWH→O3 column mappings using name, type, and context signals."""

    def __init__(
        self,
        name_weight: float = 0.6,
        type_weight: float = 0.25,
        context_weight: float = 0.15,
    ):
        self.__name_weight = name_weight
        self.__type_weight = type_weight
        self.__context_weight = context_weight

    def score(
        self,
        dwh_name: str,
        dwh_type: str,
        o3_name: str,
        o3_type: str,
        dwh_context: str | None = None,
        o3_context: str | None = None,
    ) -> MatchCandidate:
        """Score a single DWH column against a single O3 attribute."""
        name_score = self._name_similarity(dwh_name, o3_name)
        type_score = self._type_compatibility(dwh_type, o3_type)
        context_score = self._context_similarity(dwh_context, o3_context)

        composite = (
            name_score * self.__name_weight
            + type_score * self.__type_weight
            + context_score * self.__context_weight
        )

        return MatchCandidate(
            dwh_source=dwh_name,
            o3_target=o3_name,
            score=round(composite, 4),
            signals={
                "name": round(name_score, 4),
                "type": round(type_score, 4),
                "context": round(context_score, 4),
            },
        )

    def _name_similarity(self, dwh_name: str, o3_name: str) -> float:
        """Token-overlap similarity after stripping common prefixes."""
        dwh_tokens = self.__tokenize(dwh_name)
        o3_tokens = self.__tokenize(o3_name)

        if not dwh_tokens or not o3_tokens:
            return 0.0

        intersection = dwh_tokens & o3_tokens
        union = dwh_tokens | o3_tokens

        if not union:
            return 0.0

        return len(intersection) / len(union)

    def _type_compatibility(self, dwh_type: str, o3_type: str) -> float:
        """Check if DWH and O3 types belong to the same compatibility group."""
        dwh_group = _TYPE_TO_GROUP.get(dwh_type.lower())
        o3_group = _TYPE_TO_GROUP.get(o3_type.lower())

        if dwh_group is not None and dwh_group == o3_group:
            return 1.0
        if dwh_group is not None and o3_group is not None:
            return 0.0
        # One or both types unrecognized — partial credit
        return 0.3

    def _context_similarity(
        self, dwh_context: str | None, o3_context: str | None
    ) -> float:
        """Boost score if both belong to the same semantic domain."""
        if dwh_context is None or o3_context is None:
            return 0.0

        dwh_tokens = self.__tokenize(dwh_context)
        o3_tokens = self.__tokenize(o3_context)

        if not dwh_tokens or not o3_tokens:
            return 0.0

        intersection = dwh_tokens & o3_tokens
        union = dwh_tokens | o3_tokens

        return len(intersection) / len(union)

    def __tokenize(self, name: str) -> set[str]:
        """Split a name into lowercase tokens, stripping common prefixes."""
        stripped = _STRIP_PREFIXES.sub("", name)
        if not stripped:
            stripped = name
        tokens = _TOKEN_SPLIT.findall(stripped)
        return {t.lower() for t in tokens if len(t) > 1}


if __name__ == "__main__":
    pass
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_match_engine.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/mapping/match_engine.py tests/etl/test_match_engine.py
git commit -m "feat: add match engine with name/type/context scoring"
```

---

### Task 5: Mapping Store — JSON Persistence

**Files:**
- Create: `src/etl/mapping/mapping_store.py`
- Create: `tests/etl/test_mapping_store.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_mapping_store.py
import json
import os
import tempfile
import pytest
from etl.mapping.mapping_store import CrosswalkEntry, MappingStore


def _make_entry(**overrides) -> CrosswalkEntry:
    base = {
        "dwh_table": "DWH.DimPatient",
        "dwh_column": "PatientId",
        "model_name": "dwPatientModel",
        "model_alias": "PatientId",
        "model_expr": "PatientId",
        "o3_key_element": "Patient",
        "o3_attribute": "PatientIdentifier",
        "confidence": 0.92,
        "status": "auto",
    }
    base.update(overrides)
    return CrosswalkEntry(**base)


class TestCrosswalkEntry:
    def test_to_dict_roundtrip(self):
        entry = _make_entry()
        d = entry.to_dict()
        restored = CrosswalkEntry.from_dict(d)
        assert restored == entry

    def test_is_confirmed(self):
        assert _make_entry(status="confirmed").is_active
        assert _make_entry(status="auto").is_active
        assert _make_entry(status="manual").is_active
        assert not _make_entry(status="rejected").is_active


class TestMappingStore:
    def test_save_and_load(self):
        entries = [_make_entry(), _make_entry(dwh_column="DateOfBirth", o3_attribute="PatientDateOfBirth")]
        store = MappingStore()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name

        try:
            store.save(entries, path)
            loaded = store.load(path)
            assert len(loaded) == 2
            assert loaded[0].dwh_column == "PatientId"
            assert loaded[1].dwh_column == "DateOfBirth"
        finally:
            os.unlink(path)

    def test_load_empty_file(self):
        store = MappingStore()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([], f)
            path = f.name
        try:
            loaded = store.load(path)
            assert loaded == []
        finally:
            os.unlink(path)

    def test_diff(self):
        old = [_make_entry(confidence=0.9)]
        new = [
            _make_entry(confidence=0.95),
            _make_entry(dwh_column="DateOfBirth", o3_attribute="DOB"),
        ]
        store = MappingStore()
        diff = store.diff(old, new)
        assert len(diff.added) == 1
        assert len(diff.changed) == 1
        assert len(diff.removed) == 0
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_mapping_store.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/mapping/mapping_store.py
"""Persistence for crosswalk entries — load, save, diff."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict


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
    status: str  # "auto" | "confirmed" | "rejected" | "manual"

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
        return cls(**data)


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
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_mapping_store.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/mapping/mapping_store.py tests/etl/test_mapping_store.py
git commit -m "feat: add mapping store with crosswalk entry persistence and diff"
```

---

### Task 6: Crosswalk Orchestrator — Auto-Suggest & Merge

**Files:**
- Create: `src/etl/mapping/crosswalk.py`
- Create: `tests/etl/test_crosswalk.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_crosswalk.py
from unittest.mock import MagicMock, PropertyMock
import pytest
from etl.mapping.crosswalk import Crosswalk
from etl.mapping.mapping_store import CrosswalkEntry


def _mock_o3_model():
    """Mock O3DataModel with two key elements, each with attributes."""
    model = MagicMock()

    # Patient key element
    patient_ke = MagicMock()
    patient_ke.key_element_name = "Patient"
    patient_ke.string_code = "KEL_Patient"

    patient_attr = MagicMock()
    patient_attr.value_name = "PatientIdentifier"
    patient_attr.value_data_type = "String"
    patient_ke.list_attributes = [patient_attr]

    # Course key element
    course_ke = MagicMock()
    course_ke.key_element_name = "Course"
    course_ke.string_code = "KEL_Course"

    course_attr = MagicMock()
    course_attr.value_name = "CourseIdentifier"
    course_attr.value_data_type = "String"
    course_ke.list_attributes = [course_attr]

    model.key_elements = {
        "Patient": patient_ke,
        "Course": course_ke,
    }
    return model


def _mock_manifest():
    """Mock SemanticManifest with one table and one model."""
    manifest = MagicMock()

    # DWH table
    col = MagicMock()
    col.name = "PatientId"
    col.data_type = "varchar"

    table = MagicMock()
    table.full_name = "DWH.DimPatient"
    table.name = "DimPatient"
    table.type = "dimension"
    table.columns = [col]

    manifest.tables = {"DWH.DimPatient": table}

    # Conceptual model
    select = MagicMock()
    select.alias = "PatientId"
    select.from_table = "Patient"
    select.expr = "PatientId"
    select.data_type = "VDT_PATIENTID"

    model = MagicMock()
    model.name = "dwPatientModel"
    model.base_table = "DWH.FactPatientPayor"
    model.selects = [select]

    manifest.models = [model]
    return manifest


def _mock_registry():
    """Mock ModelRegistry with one entry point."""
    registry = MagicMock()
    ep = MagicMock()
    ep.base_table = "DWH.FactPatientPayor"
    ep.preferred_conceptual_model = "dwPatientModel"
    registry.entry_points = {"patient": ep}
    registry.field_policy_defaults = MagicMock()
    registry.field_policy_defaults.deny_list = ["PatientSSN"]
    return registry


class TestGenerateSuggestions:
    def test_returns_crosswalk_entries(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        assert len(suggestions) > 0
        assert all(isinstance(s, CrosswalkEntry) for s in suggestions)

    def test_suggestions_have_auto_status(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        assert all(s.status == "auto" for s in suggestions)

    def test_denied_columns_excluded(self):
        manifest = _mock_manifest()
        ssn_col = MagicMock()
        ssn_col.name = "PatientSSN"
        ssn_col.data_type = "varchar"
        manifest.tables["DWH.DimPatient"].columns.append(ssn_col)

        cw = Crosswalk(manifest, _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        assert not any(s.dwh_column == "PatientSSN" for s in suggestions)

    def test_suggestions_sorted_by_confidence(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        scores = [s.confidence for s in suggestions]
        assert scores == sorted(scores, reverse=True)


class TestMerge:
    def test_curated_overrides_suggestions(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())

        suggestions = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="auto",
            )
        ]
        curated = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="confirmed",
            )
        ]

        merged = cw.merge(suggestions, curated)
        assert len(merged) == 1
        assert merged[0].status == "confirmed"

    def test_rejected_entries_excluded_from_active(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())

        suggestions = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="auto",
            )
        ]
        curated = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="rejected",
            )
        ]

        merged = cw.merge(suggestions, curated)
        assert merged[0].status == "rejected"
        assert not merged[0].is_active
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_crosswalk.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/mapping/crosswalk.py
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
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_crosswalk.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/mapping/crosswalk.py tests/etl/test_crosswalk.py
git commit -m "feat: add crosswalk orchestrator with auto-suggest and merge"
```

---

### Task 7: Lineage Builder — Graph Construction

**Files:**
- Create: `src/etl/lineage/lineage_builder.py`
- Create: `tests/etl/test_lineage_builder.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_lineage_builder.py
from unittest.mock import MagicMock
import pytest
from etl.lineage.lineage_builder import LineageBuilder, LineageGraph, LineageNode
from etl.mapping.mapping_store import CrosswalkEntry


def _make_entry(**overrides) -> CrosswalkEntry:
    base = {
        "dwh_table": "DWH.DimPatient",
        "dwh_column": "PatientId",
        "model_name": None,
        "model_alias": None,
        "model_expr": None,
        "o3_key_element": "Patient",
        "o3_attribute": "PatientIdentifier",
        "confidence": 0.92,
        "status": "confirmed",
    }
    base.update(overrides)
    return CrosswalkEntry(**base)


def _mock_o3_model():
    model = MagicMock()
    ke = MagicMock()
    ke.key_element_name = "Patient"
    attr = MagicMock()
    attr.value_name = "PatientIdentifier"
    attr.value_data_type = "String"
    attr2 = MagicMock()
    attr2.value_name = "PatientDateOfBirth"
    attr2.value_data_type = "Date"
    ke.list_attributes = [attr, attr2]
    model.key_elements = {"Patient": ke}
    return model


def _mock_manifest():
    manifest = MagicMock()
    col = MagicMock()
    col.name = "PatientId"
    col.data_type = "varchar"
    col2 = MagicMock()
    col2.name = "DateOfBirth"
    col2.data_type = "datetime"
    table = MagicMock()
    table.full_name = "DWH.DimPatient"
    table.columns = [col, col2]
    manifest.tables = {"DWH.DimPatient": table}
    manifest.models = []
    return manifest


class TestLineageBuilder:
    def test_build_direct_mapping(self):
        entries = [_make_entry()]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        assert isinstance(graph, LineageGraph)
        assert len(graph.nodes) >= 2  # source + target
        assert len(graph.edges) >= 1

    def test_build_with_transform(self):
        entries = [
            _make_entry(
                model_name="dwPatientModel",
                model_alias="PatientId",
                model_expr="UPPER(PatientId)",
            )
        ]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        transform_nodes = [n for n in graph.nodes if n.node_type == "transform"]
        assert len(transform_nodes) == 1
        assert "UPPER(PatientId)" in transform_nodes[0].metadata.get("expr", "")

    def test_trace_forward(self):
        entries = [_make_entry()]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        targets = graph.trace_forward("DWH.DimPatient", "PatientId")
        assert any(n.column == "PatientIdentifier" for n in targets)

    def test_trace_backward(self):
        entries = [_make_entry()]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        sources = graph.trace_backward("Patient", "PatientIdentifier")
        assert any(n.column == "PatientId" for n in sources)

    def test_unmapped_targets(self):
        entries = [_make_entry()]  # only PatientIdentifier mapped
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        unmapped = graph.unmapped_targets()
        assert any(n.column == "PatientDateOfBirth" for n in unmapped)

    def test_rejected_entries_excluded(self):
        entries = [_make_entry(status="rejected")]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        assert len(graph.edges) == 0
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_lineage_builder.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/lineage/lineage_builder.py
"""Builds a directed lineage graph from crosswalk entries."""

from __future__ import annotations

from dataclasses import dataclass, field

from etl.mapping.mapping_store import CrosswalkEntry
from etl.manifest import SemanticManifest


@dataclass(frozen=True)
class LineageNode:
    """A node in the lineage graph: source, transform, or target."""

    node_type: str  # "source" | "transform" | "target"
    table: str
    column: str
    metadata: dict = field(default_factory=dict, hash=False, compare=False)


@dataclass
class LineageEdge:
    """A directed edge in the lineage graph."""

    source: LineageNode
    target: LineageNode
    transform_expr: str | None = None
    model_name: str | None = None
    confidence: float = 0.0


@dataclass
class LineageGraph:
    """Directed graph of data lineage: source → (transform) → target."""

    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)

    def __post_init__(self):
        self.__adjacency: dict[LineageNode, list[LineageEdge]] = {}
        self.__reverse: dict[LineageNode, list[LineageEdge]] = {}
        for edge in self.edges:
            self.__adjacency.setdefault(edge.source, []).append(edge)
            self.__reverse.setdefault(edge.target, []).append(edge)

    def trace_forward(self, source_table: str, source_column: str) -> list[LineageNode]:
        """Find all target nodes reachable from a source column."""
        start = LineageNode(node_type="source", table=source_table, column=source_column)
        visited: set[LineageNode] = set()
        targets: list[LineageNode] = []
        queue = [start]

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            if current.node_type == "target":
                targets.append(current)
            for edge in self.__adjacency.get(current, []):
                queue.append(edge.target)

        return targets

    def trace_backward(self, o3_element: str, o3_attribute: str) -> list[LineageNode]:
        """Find all source nodes that feed an O3 attribute."""
        target = LineageNode(node_type="target", table=o3_element, column=o3_attribute)
        visited: set[LineageNode] = set()
        sources: list[LineageNode] = []
        queue = [target]

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            if current.node_type == "source":
                sources.append(current)
            for edge in self.__reverse.get(current, []):
                queue.append(edge.source)

        return sources

    def unmapped_sources(self) -> list[LineageNode]:
        """DWH source nodes with no outgoing edges."""
        source_nodes = {n for n in self.nodes if n.node_type == "source"}
        connected = {e.source for e in self.edges}
        return sorted(source_nodes - connected, key=lambda n: (n.table, n.column))

    def unmapped_targets(self) -> list[LineageNode]:
        """O3 target nodes with no incoming edges."""
        target_nodes = {n for n in self.nodes if n.node_type == "target"}
        connected = {e.target for e in self.edges}
        return sorted(target_nodes - connected, key=lambda n: (n.table, n.column))


class LineageBuilder:
    """Constructs a LineageGraph from crosswalk entries and manifest data."""

    def __init__(
        self,
        crosswalk: list[CrosswalkEntry],
        manifest: SemanticManifest,
        o3_model,
    ):
        self.__crosswalk = crosswalk
        self.__manifest = manifest
        self.__o3_model = o3_model

    def build(self) -> LineageGraph:
        nodes: set[LineageNode] = set()
        edges: list[LineageEdge] = []

        # Add all O3 attributes as target nodes
        for ke_name, key_element in self.__o3_model.key_elements.items():
            for attr in key_element.list_attributes:
                nodes.add(
                    LineageNode(
                        node_type="target",
                        table=ke_name,
                        column=attr.value_name,
                        metadata={"data_type": attr.value_data_type},
                    )
                )

        # Add all DWH columns as source nodes
        for table in self.__manifest.tables.values():
            for col in table.columns:
                nodes.add(
                    LineageNode(
                        node_type="source",
                        table=table.full_name,
                        column=col.name,
                        metadata={"data_type": col.data_type},
                    )
                )

        # Build edges from active crosswalk entries
        active = [e for e in self.__crosswalk if e.is_active]
        for entry in active:
            source = LineageNode(
                node_type="source",
                table=entry.dwh_table,
                column=entry.dwh_column,
            )
            target = LineageNode(
                node_type="target",
                table=entry.o3_key_element,
                column=entry.o3_attribute,
            )
            nodes.add(source)
            nodes.add(target)

            if entry.model_expr and entry.model_expr != entry.dwh_column:
                # Insert transform node
                transform = LineageNode(
                    node_type="transform",
                    table=entry.model_name or "",
                    column=entry.model_alias or entry.dwh_column,
                    metadata={"expr": entry.model_expr},
                )
                nodes.add(transform)
                edges.append(
                    LineageEdge(
                        source=source,
                        target=transform,
                        model_name=entry.model_name,
                        confidence=entry.confidence,
                    )
                )
                edges.append(
                    LineageEdge(
                        source=transform,
                        target=target,
                        transform_expr=entry.model_expr,
                        model_name=entry.model_name,
                        confidence=entry.confidence,
                    )
                )
            else:
                edges.append(
                    LineageEdge(
                        source=source,
                        target=target,
                        model_name=entry.model_name,
                        confidence=entry.confidence,
                    )
                )

        graph = LineageGraph(nodes=list(nodes), edges=edges)
        return graph


if __name__ == "__main__":
    pass
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_lineage_builder.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/lineage/lineage_builder.py tests/etl/test_lineage_builder.py
git commit -m "feat: add lineage builder with directed graph and traversal"
```

---

### Task 8: Lineage Report — Export JSON, Markdown, Coverage

**Files:**
- Create: `src/etl/lineage/lineage_report.py`
- Create: `tests/etl/test_lineage_report.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_lineage_report.py
import json
import os
import tempfile
import pytest
from etl.lineage.lineage_builder import LineageGraph, LineageNode, LineageEdge
from etl.lineage.lineage_report import LineageReport


def _make_graph() -> LineageGraph:
    source = LineageNode("source", "DWH.DimPatient", "PatientId", {"data_type": "varchar"})
    target_mapped = LineageNode("target", "Patient", "PatientIdentifier", {"data_type": "String"})
    target_unmapped = LineageNode("target", "Patient", "PatientDateOfBirth", {"data_type": "Date"})
    source_unmapped = LineageNode("source", "DWH.DimPatient", "PatientSSN", {"data_type": "varchar"})
    edge = LineageEdge(source=source, target=target_mapped, confidence=0.92)

    return LineageGraph(
        nodes=[source, target_mapped, target_unmapped, source_unmapped],
        edges=[edge],
    )


class TestCoverageSummary:
    def test_counts(self):
        report = LineageReport(_make_graph())
        summary = report.coverage_summary()
        assert summary["total_o3_attributes"] == 2
        assert summary["mapped"] == 1
        assert summary["unmapped"] == 1
        assert summary["coverage_pct"] == 50.0

    def test_empty_graph(self):
        report = LineageReport(LineageGraph())
        summary = report.coverage_summary()
        assert summary["total_o3_attributes"] == 0
        assert summary["coverage_pct"] == 0.0


class TestToJson:
    def test_writes_valid_json(self):
        report = LineageReport(_make_graph())
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name
        try:
            report.to_json(path)
            with open(path) as f:
                data = json.load(f)
            assert "nodes" in data
            assert "edges" in data
            assert len(data["nodes"]) == 4
            assert len(data["edges"]) == 1
        finally:
            os.unlink(path)


class TestToMarkdown:
    def test_writes_markdown(self):
        report = LineageReport(_make_graph())
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            path = f.name
        try:
            report.to_markdown(path)
            with open(path) as f:
                content = f.read()
            assert "Patient" in content
            assert "PatientIdentifier" in content
            assert "PatientDateOfBirth" in content  # in unmapped section
            assert "|" in content  # table format
        finally:
            os.unlink(path)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_lineage_report.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/lineage/lineage_report.py
"""Export lineage graphs as JSON, markdown, and coverage summaries."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict

from etl.lineage.lineage_builder import LineageGraph, LineageNode


class LineageReport:
    """Generates reports from a LineageGraph."""

    def __init__(self, graph: LineageGraph):
        self.__graph = graph

    def coverage_summary(self) -> dict:
        """Return coverage statistics for O3 attributes."""
        targets = [n for n in self.__graph.nodes if n.node_type == "target"]
        connected_targets = {e.target for e in self.__graph.edges}
        mapped = [t for t in targets if t in connected_targets]

        total = len(targets)
        mapped_count = len(mapped)
        unmapped_count = total - mapped_count

        return {
            "total_o3_attributes": total,
            "mapped": mapped_count,
            "unmapped": unmapped_count,
            "coverage_pct": round(mapped_count / total * 100, 1) if total else 0.0,
        }

    def to_json(self, path: str) -> None:
        """Write the full lineage graph as JSON."""
        data = {
            "nodes": [
                {
                    "node_type": n.node_type,
                    "table": n.table,
                    "column": n.column,
                    "metadata": n.metadata,
                }
                for n in self.__graph.nodes
            ],
            "edges": [
                {
                    "source_table": e.source.table,
                    "source_column": e.source.column,
                    "target_table": e.target.table,
                    "target_column": e.target.column,
                    "transform_expr": e.transform_expr,
                    "model_name": e.model_name,
                    "confidence": e.confidence,
                }
                for e in self.__graph.edges
            ],
            "coverage": self.coverage_summary(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def to_markdown(self, path: str) -> None:
        """Write a human-readable markdown lineage report."""
        lines: list[str] = ["# Data Lineage Report\n"]

        # Coverage summary
        summary = self.coverage_summary()
        lines.append("## Coverage Summary\n")
        lines.append(f"- **Total O3 Attributes:** {summary['total_o3_attributes']}")
        lines.append(f"- **Mapped:** {summary['mapped']}")
        lines.append(f"- **Unmapped:** {summary['unmapped']}")
        lines.append(f"- **Coverage:** {summary['coverage_pct']}%\n")

        # Group edges by O3 key element
        edges_by_element: dict[str, list] = defaultdict(list)
        for edge in self.__graph.edges:
            if edge.target.node_type == "target":
                edges_by_element[edge.target.table].append(edge)
            elif edge.source.node_type == "transform":
                # Transform → target edge: group by target
                edges_by_element[edge.target.table].append(edge)

        # Mapped attributes table per key element
        if edges_by_element:
            lines.append("## Mapped Attributes\n")
            for element in sorted(edges_by_element.keys()):
                lines.append(f"### {element}\n")
                lines.append("| O3 Attribute | DWH Source | Transform | Model | Confidence |")
                lines.append("|---|---|---|---|---|")
                for edge in edges_by_element[element]:
                    source = f"{edge.source.table}.{edge.source.column}"
                    transform = edge.transform_expr or "— (direct)"
                    model = edge.model_name or "—"
                    lines.append(
                        f"| {edge.target.column} | {source} | {transform} | {model} | {edge.confidence} |"
                    )
                lines.append("")

        # Unmapped O3 attributes
        unmapped_targets = self.__graph.unmapped_targets()
        if unmapped_targets:
            lines.append("## Unmapped O3 Attributes\n")
            for node in unmapped_targets:
                lines.append(f"- {node.table}.{node.column}")
            lines.append("")

        # Unmapped DWH columns
        unmapped_sources = self.__graph.unmapped_sources()
        if unmapped_sources:
            lines.append("## Unmapped DWH Columns\n")
            for node in unmapped_sources:
                lines.append(f"- {node.table}.{node.column}")
            lines.append("")

        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))


if __name__ == "__main__":
    pass
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_lineage_report.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/lineage/lineage_report.py tests/etl/test_lineage_report.py
git commit -m "feat: add lineage report with JSON, markdown, and coverage export"
```

---

### Task 9: Extractor — SELECT SQL Generation

**Files:**
- Create: `src/etl/pipeline/extractor.py`
- Create: `tests/etl/test_extractor.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_extractor.py
from unittest.mock import MagicMock
import pytest
from etl.pipeline.extractor import ExtractQuery, Extractor
from etl.mapping.mapping_store import CrosswalkEntry
from etl.registry import (
    DateBasis, EntryPoint, FieldPolicy, GlobalPolicy, DateRangePolicy,
    JoinPolicy, JoinSpec, ModelConfig, ModelRegistry, QuerySafety, TimePolicy,
)
from etl.manifest import SemanticManifest, DWHTable, Column, ConceptualModel, ModelSelect


def _make_entry(**overrides) -> CrosswalkEntry:
    base = {
        "dwh_table": "DWH.FactActivityBilling",
        "dwh_column": "DimPatientID",
        "model_name": "dwActivityBillingModel",
        "model_alias": "PatientId",
        "model_expr": "PatientId",
        "o3_key_element": "Patient",
        "o3_attribute": "PatientIdentifier",
        "confidence": 0.92,
        "status": "confirmed",
    }
    base.update(overrides)
    return CrosswalkEntry(**base)


def _make_registry() -> ModelRegistry:
    return ModelRegistry(
        entry_points={
            "billing": EntryPoint(
                base_table="DWH.FactActivityBilling",
                preferred_conceptual_model="dwActivityBillingModel",
                time_policy=TimePolicy(
                    default_date_key="DimDateID_FromDateOfService",
                    date_basis=DateBasis(
                        enum=["service"],
                        map={"service": "DimDateID_FromDateOfService"},
                        default="service",
                    ),
                    default_lookback_days=90,
                ),
            )
        },
        models={
            "dwActivityBillingModel": ModelConfig(
                base_table="DWH.FactActivityBilling",
                join_policy=JoinPolicy(
                    mode="facts-first",
                    allowed_dimension_joins=[
                        JoinSpec("DWH.DimPatient", "DimPatientID", "DimPatientID"),
                    ],
                ),
                time_policy=TimePolicy(
                    default_date_key="DimDateID_FromDateOfService",
                    date_key_candidates=["DimDateID_FromDateOfService"],
                    date_basis=DateBasis(
                        enum=["service"],
                        map={"service": "DimDateID_FromDateOfService"},
                        default="service",
                    ),
                ),
                field_policy=FieldPolicy(deny_list=["PatientSSN"]),
            )
        },
        global_policy=GlobalPolicy(
            timezone="America/New_York",
            date_range=DateRangePolicy("inclusive", ["inclusive"]),
            query_safety=QuerySafety(
                select_only=True,
                default_row_limit=1000,
                max_row_limit=100000,
                require_date_filter_for_tables=["DWH.FactActivityBilling"],
                cross_fact_joins="disallow_unless_bridge",
            ),
        ),
        field_policy_defaults=FieldPolicy(deny_list=["PatientSSN"]),
    )


def _make_manifest() -> MagicMock:
    manifest = MagicMock(spec=SemanticManifest)
    col = MagicMock(spec=Column)
    col.name = "DimPatientID"
    col.data_type = "int"
    table = MagicMock(spec=DWHTable)
    table.full_name = "DWH.FactActivityBilling"
    table.columns = [col]
    table.columns_by_name = {"DimPatientID": col}
    manifest.tables = {"DWH.FactActivityBilling": table}
    manifest.models = []
    manifest.models_by_name = {}
    return manifest


class TestGenerateQuery:
    def test_returns_extract_query(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert isinstance(query, ExtractQuery)
        assert query.entry_point == "billing"
        assert query.base_table == "DWH.FactActivityBilling"
        assert "SELECT" in query.sql
        assert "FROM" in query.sql

    def test_includes_date_filter(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "DimDateID_FromDateOfService" in query.sql
        assert "WHERE" in query.sql

    def test_includes_row_limit(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "TOP" in query.sql or "LIMIT" in query.sql

    def test_custom_lookback_days(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing", lookback_days=30)
        assert "30" in query.sql

    def test_denied_columns_excluded(self):
        entries = [_make_entry(dwh_column="PatientSSN", model_alias="PatientSSN")]
        extractor = Extractor(entries, _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "PatientSSN" not in query.sql

    def test_invalid_entry_point_raises(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        with pytest.raises(ValueError, match="entry point"):
            extractor.generate_query("nonexistent")

    def test_includes_join_for_dimension(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "JOIN" in query.sql or "DimPatient" in query.sql


class TestGenerateAllQueries:
    def test_returns_queries_for_all_entry_points(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        queries = extractor.generate_all_queries()
        assert len(queries) == 1
        assert queries[0].entry_point == "billing"
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_extractor.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/pipeline/extractor.py
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

        lookback = lookback_days or time_policy.default_lookback_days or 90

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
            model_entries = relevant

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

        if not select_columns:
            select_columns = ["  base.*"]

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

    def generate_all_queries(self) -> list[ExtractQuery]:
        """Generate extract queries for all entry points."""
        return [
            self.generate_query(ep)
            for ep in self.__registry.entry_points
        ]


if __name__ == "__main__":
    pass
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_extractor.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/pipeline/extractor.py tests/etl/test_extractor.py
git commit -m "feat: add extractor with policy-aware SELECT generation"
```

---

### Task 10: Loader — INSERT/MERGE Generation

**Files:**
- Create: `src/etl/pipeline/loader.py`
- Create: `tests/etl/test_loader.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_loader.py
from unittest.mock import MagicMock
import pytest
from etl.pipeline.loader import LoadCommand, Loader
from etl.pipeline.extractor import ExtractQuery
from etl.mapping.mapping_store import CrosswalkEntry
from etl.registry import JoinSpec
from helpers.enums import SupportedSQLServers


def _make_entry(**overrides) -> CrosswalkEntry:
    base = {
        "dwh_table": "DWH.FactActivityBilling",
        "dwh_column": "DimPatientID",
        "model_name": "dwActivityBillingModel",
        "model_alias": "PatientId",
        "model_expr": "PatientId",
        "o3_key_element": "Patient",
        "o3_attribute": "PatientIdentifier",
        "confidence": 0.92,
        "status": "confirmed",
    }
    base.update(overrides)
    return CrosswalkEntry(**base)


def _make_extract_query(entries=None) -> ExtractQuery:
    return ExtractQuery(
        entry_point="billing",
        model_name="dwActivityBillingModel",
        base_table="DWH.FactActivityBilling",
        sql="SELECT ...",
        date_key="DimDateID_FromDateOfService",
        joins=[],
        columns_mapped=entries or [_make_entry()],
    )


def _mock_o3_model():
    model = MagicMock()
    ke = MagicMock()
    ke.string_code = "KEL_Patient"
    ke.key_element_name = "Patient"
    model.key_elements = {"Patient": ke}
    return model


class TestGenerateInsert:
    def test_returns_load_command(self):
        loader = Loader([_make_entry()], _mock_o3_model(), SupportedSQLServers.MSSQL)
        cmd = loader.generate_insert(_make_extract_query())
        assert isinstance(cmd, LoadCommand)
        assert "INSERT" in cmd.sql
        assert "Patient" in cmd.target_table

    def test_column_map_populated(self):
        loader = Loader([_make_entry()], _mock_o3_model(), SupportedSQLServers.MSSQL)
        cmd = loader.generate_insert(_make_extract_query())
        assert "PatientIdentifier" in cmd.column_map


class TestGenerateMerge:
    def test_returns_merge_command(self):
        loader = Loader([_make_entry()], _mock_o3_model(), SupportedSQLServers.MSSQL)
        cmd = loader.generate_merge(_make_extract_query(), merge_key=["PatientIdentifier"])
        assert isinstance(cmd, LoadCommand)
        assert "MERGE" in cmd.sql

    def test_merge_key_in_on_clause(self):
        loader = Loader([_make_entry()], _mock_o3_model(), SupportedSQLServers.MSSQL)
        cmd = loader.generate_merge(_make_extract_query(), merge_key=["PatientIdentifier"])
        assert "PatientIdentifier" in cmd.sql
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_loader.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/pipeline/loader.py
"""Generates INSERT and MERGE SQL to load extracted data into O3 tables."""

from __future__ import annotations

from dataclasses import dataclass

from etl.mapping.mapping_store import CrosswalkEntry
from etl.pipeline.extractor import ExtractQuery
from helpers.enums import SupportedSQLServers
from helpers.string_helpers import leave_only_letters_numbers_or_underscore


@dataclass
class LoadCommand:
    """A generated load SQL statement."""

    target_table: str
    sql: str
    column_map: dict[str, str]  # {o3_column: source_alias}


class Loader:
    """Generates INSERT/MERGE SQL for loading into O3 tables."""

    def __init__(
        self,
        crosswalk: list[CrosswalkEntry],
        o3_model,
        sql_server_type: SupportedSQLServers,
    ):
        self.__crosswalk = crosswalk
        self.__o3_model = o3_model
        self.__sql_server_type = sql_server_type

    def generate_insert(self, extract: ExtractQuery) -> LoadCommand:
        """Generate an INSERT INTO ... SELECT statement."""
        column_map, target_table = self.__build_column_map(extract)

        if not column_map:
            return LoadCommand(
                target_table=target_table,
                sql=f"-- No mapped columns for {target_table}",
                column_map={},
            )

        o3_columns = ", ".join(f"[{col}]" for col in column_map.keys())
        source_columns = ", ".join(
            f"src.[{alias}]" for alias in column_map.values()
        )

        sql = (
            f"INSERT INTO {target_table} ({o3_columns})\n"
            f"SELECT {source_columns}\n"
            f"FROM (\n{extract.sql}\n) AS src"
        )

        return LoadCommand(target_table=target_table, sql=sql, column_map=column_map)

    def generate_merge(
        self, extract: ExtractQuery, merge_key: list[str]
    ) -> LoadCommand:
        """Generate a MERGE (upsert) statement."""
        column_map, target_table = self.__build_column_map(extract)

        if not column_map:
            return LoadCommand(
                target_table=target_table,
                sql=f"-- No mapped columns for {target_table}",
                column_map={},
            )

        on_clause = " AND ".join(
            f"target.[{k}] = source.[{column_map[k]}]"
            for k in merge_key
            if k in column_map
        )

        update_cols = [
            f"target.[{k}] = source.[{v}]"
            for k, v in column_map.items()
            if k not in merge_key
        ]

        insert_cols = ", ".join(f"[{k}]" for k in column_map.keys())
        insert_vals = ", ".join(f"source.[{v}]" for v in column_map.values())

        sql_parts = [
            f"MERGE {target_table} AS target",
            f"USING (\n{extract.sql}\n) AS source",
            f"ON {on_clause}",
        ]

        if update_cols:
            sql_parts.append(
                "WHEN MATCHED THEN UPDATE SET\n  " + ",\n  ".join(update_cols)
            )

        sql_parts.append(
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols})\n"
            f"  VALUES ({insert_vals});"
        )

        sql = "\n".join(sql_parts)

        return LoadCommand(target_table=target_table, sql=sql, column_map=column_map)

    def __build_column_map(
        self, extract: ExtractQuery
    ) -> tuple[dict[str, str], str]:
        """Build {o3_column: source_alias} map and determine target table."""
        column_map: dict[str, str] = {}
        target_elements: set[str] = set()

        for entry in extract.columns_mapped:
            if not entry.is_active:
                continue
            o3_col = leave_only_letters_numbers_or_underscore(entry.o3_attribute)
            source_alias = entry.model_alias or entry.dwh_column
            column_map[o3_col] = source_alias
            target_elements.add(entry.o3_key_element)

        # Use the first O3 key element as the target table
        if target_elements:
            element_name = sorted(target_elements)[0]
            ke = self.__o3_model.key_elements.get(element_name)
            if ke:
                table_name = leave_only_letters_numbers_or_underscore(ke.string_code)
            else:
                table_name = leave_only_letters_numbers_or_underscore(element_name)
        else:
            table_name = "unknown"

        return column_map, table_name


if __name__ == "__main__":
    pass
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_loader.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/pipeline/loader.py tests/etl/test_loader.py
git commit -m "feat: add loader with INSERT and MERGE SQL generation"
```

---

### Task 11: Runner — ETL Orchestration (Live + Offline)

**Files:**
- Create: `src/etl/pipeline/runner.py`
- Create: `tests/etl/test_runner.py`

**Step 1: Write the failing test**

```python
# tests/etl/test_runner.py
import os
import tempfile
from unittest.mock import MagicMock, patch, PropertyMock
import pytest
from etl.pipeline.runner import ETLRunner, ETLResult, EntryPointResult
from etl.pipeline.extractor import ExtractQuery, Extractor
from etl.pipeline.loader import LoadCommand, Loader
from etl.mapping.mapping_store import CrosswalkEntry


def _make_extract_query() -> ExtractQuery:
    return ExtractQuery(
        entry_point="billing",
        model_name="dwActivityBillingModel",
        base_table="DWH.FactActivityBilling",
        sql="SELECT TOP 1000 base.[DimPatientID] FROM DWH.FactActivityBilling AS base",
        date_key="DimDateID_FromDateOfService",
        joins=[],
        columns_mapped=[],
    )


def _make_load_command() -> LoadCommand:
    return LoadCommand(
        target_table="KEL_Patient",
        sql="INSERT INTO KEL_Patient ([PatientIdentifier]) SELECT src.[PatientId] FROM (...) AS src",
        column_map={"PatientIdentifier": "PatientId"},
    )


def _mock_extractor() -> MagicMock:
    ext = MagicMock(spec=Extractor)
    ext.generate_query.return_value = _make_extract_query()
    ext.generate_all_queries.return_value = [_make_extract_query()]
    return ext


def _mock_loader() -> MagicMock:
    ldr = MagicMock(spec=Loader)
    ldr.generate_insert.return_value = _make_load_command()
    return ldr


class TestExportSql:
    def test_writes_sql_files(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with tempfile.TemporaryDirectory() as tmpdir:
            runner.export_sql(tmpdir)
            files = os.listdir(tmpdir)
            assert len(files) > 0
            assert any(f.endswith(".sql") for f in files)

    def test_sql_file_contains_extract_and_load(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with tempfile.TemporaryDirectory() as tmpdir:
            runner.export_sql(tmpdir)
            sql_files = [f for f in os.listdir(tmpdir) if f.endswith(".sql")]
            with open(os.path.join(tmpdir, sql_files[0])) as f:
                content = f.read()
            assert "SELECT" in content
            assert "INSERT" in content

    def test_filter_by_entry_points(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with tempfile.TemporaryDirectory() as tmpdir:
            runner.export_sql(tmpdir, entry_points=["billing"])
            files = os.listdir(tmpdir)
            assert len(files) == 1


class TestRunDryRun:
    def test_dry_run_returns_result_without_executing(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        result = runner.run(entry_points=["billing"], dry_run=True)
        assert isinstance(result, ETLResult)
        assert result.success is True
        assert len(result.results) == 1
        assert result.results[0].entry_point == "billing"
        assert result.results[0].rows_extracted == 0
        assert result.results[0].rows_loaded == 0


class TestRunLive:
    def test_live_requires_connection(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with pytest.raises(ValueError, match="connection"):
            runner.run(entry_points=["billing"], dry_run=False)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_runner.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/etl/pipeline/runner.py
"""ETL orchestration: run live against database or export SQL files offline."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from etl.pipeline.extractor import Extractor, ExtractQuery
from etl.pipeline.loader import Loader, LoadCommand


@dataclass
class EntryPointResult:
    """Result of running ETL for a single entry point."""

    entry_point: str
    rows_extracted: int = 0
    rows_loaded: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


@dataclass
class ETLResult:
    """Result of a complete ETL run."""

    results: list[EntryPointResult] = field(default_factory=list)
    total_duration: float = 0.0
    success: bool = True


class ETLRunner:
    """Orchestrates extract-transform-load: live execution or offline SQL export."""

    def __init__(
        self,
        extractor: Extractor,
        loader: Loader,
        connection=None,
    ):
        self.__extractor = extractor
        self.__loader = loader
        self.__connection = connection

    def export_sql(
        self,
        output_dir: str,
        entry_points: list[str] | None = None,
    ) -> None:
        """Write extract + load SQL to files in output_dir."""
        os.makedirs(output_dir, exist_ok=True)

        if entry_points:
            queries = [
                self.__extractor.generate_query(ep) for ep in entry_points
            ]
        else:
            queries = self.__extractor.generate_all_queries()

        for query in queries:
            load_cmd = self.__loader.generate_insert(query)
            filename = f"{query.entry_point}_etl.sql"
            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"-- ETL: {query.entry_point}\n")
                f.write(f"-- Model: {query.model_name}\n")
                f.write(f"-- Base Table: {query.base_table}\n")
                f.write(f"-- Date Key: {query.date_key}\n\n")
                f.write("-- ===== EXTRACT =====\n\n")
                f.write(query.sql)
                f.write("\n\n-- ===== LOAD =====\n\n")
                f.write(load_cmd.sql)
                f.write("\n")

    def run(
        self,
        entry_points: list[str] | None = None,
        date_basis: str | None = None,
        lookback_days: int | None = None,
        dry_run: bool = False,
    ) -> ETLResult:
        """Execute ETL pipeline. Requires connection unless dry_run=True."""
        if not dry_run and self.__connection is None:
            raise ValueError(
                "Live execution requires a database connection. "
                "Pass connection to ETLRunner or use dry_run=True."
            )

        start = time.time()
        results: list[EntryPointResult] = []

        if entry_points:
            queries = [
                self.__extractor.generate_query(ep, date_basis, lookback_days)
                for ep in entry_points
            ]
        else:
            queries = self.__extractor.generate_all_queries()

        for query in queries:
            ep_result = self.__run_entry_point(query, dry_run)
            results.append(ep_result)

        total_duration = time.time() - start
        success = all(len(r.errors) == 0 for r in results)

        return ETLResult(
            results=results,
            total_duration=total_duration,
            success=success,
        )

    def __run_entry_point(
        self, query: ExtractQuery, dry_run: bool
    ) -> EntryPointResult:
        """Run ETL for a single entry point."""
        ep_start = time.time()
        result = EntryPointResult(entry_point=query.entry_point)

        if dry_run:
            result.duration_seconds = time.time() - ep_start
            return result

        try:
            cursor = self.__connection.cursor()

            # Extract
            cursor.execute(query.sql)
            rows = cursor.fetchall()
            result.rows_extracted = len(rows)

            # Load
            load_cmd = self.__loader.generate_insert(query)
            cursor.execute(load_cmd.sql)
            result.rows_loaded = cursor.rowcount

            self.__connection.commit()
        except Exception as e:
            result.errors.append(str(e))

        result.duration_seconds = time.time() - ep_start
        return result


if __name__ == "__main__":
    pass
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/etl/test_runner.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/etl/pipeline/runner.py tests/etl/test_runner.py
git commit -m "feat: add ETL runner with live execution and offline SQL export"
```

---

### Task 12: Integration — Full ETL Entry Point

**Files:**
- Create: `src/etl_main.py`

**Step 1: Write the integration script**

```python
# src/etl_main.py
"""ETL entry point — demonstrates the full crosswalk → lineage → pipeline workflow."""

import os
from api.data_model import O3DataModel
from etl.registry import load_model_registry
from etl.manifest import load_semantic_manifest
from etl.mapping.crosswalk import Crosswalk
from etl.lineage.lineage_builder import LineageBuilder
from etl.lineage.lineage_report import LineageReport
from etl.pipeline.extractor import Extractor
from etl.pipeline.loader import Loader
from etl.pipeline.runner import ETLRunner
from helpers.enums import SupportedSQLServers


RESOURCES = os.path.join(os.path.dirname(__file__), "Resources")
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "Sql_Commands", "etl")
CROSSWALK_PATH = os.path.join(RESOURCES, "crosswalk.json")


def main():
    # 1. Load data sources
    print("Loading O3 data model...")
    o3 = O3DataModel(os.path.join(RESOURCES, "O3_20250128_Fixed.json"), clean=True)

    print("Loading model registry...")
    registry = load_model_registry(os.path.join(RESOURCES, "model_registry.json"))

    print("Loading semantic manifests...")
    manifest = load_semantic_manifest(
        os.path.join(RESOURCES, "semantic_manifest_from_variandw_schema.json"),
        os.path.join(RESOURCES, "semantic_manifest_with_models.json"),
    )

    # 2. Generate or load crosswalk
    cw = Crosswalk(manifest, registry, o3)

    if os.path.exists(CROSSWALK_PATH):
        print(f"Loading curated crosswalk from {CROSSWALK_PATH}...")
        entries = cw.load_curated(CROSSWALK_PATH)
        print(f"  {len(entries)} curated entries loaded.")
    else:
        print("Generating crosswalk suggestions...")
        entries = cw.generate_suggestions()
        cw.save_curated(entries, CROSSWALK_PATH)
        print(f"  {len(entries)} suggestions saved to {CROSSWALK_PATH}")
        print("  Review and edit crosswalk.json, then re-run.")

    # 3. Build lineage
    print("Building lineage graph...")
    graph = LineageBuilder(entries, manifest, o3).build()
    report = LineageReport(graph)

    summary = report.coverage_summary()
    print(f"  Coverage: {summary['mapped']}/{summary['total_o3_attributes']} "
          f"({summary['coverage_pct']}%)")

    lineage_json = os.path.join(OUTPUT, "lineage.json")
    lineage_md = os.path.join(OUTPUT, "lineage.md")
    os.makedirs(OUTPUT, exist_ok=True)
    report.to_json(lineage_json)
    report.to_markdown(lineage_md)
    print(f"  Lineage exported to {lineage_json} and {lineage_md}")

    # 4. Export ETL SQL (offline mode)
    print("Generating ETL SQL...")
    active_entries = [e for e in entries if e.is_active]
    extractor = Extractor(active_entries, manifest, registry)
    loader = Loader(active_entries, o3, SupportedSQLServers.MSSQL)
    runner = ETLRunner(extractor, loader)
    runner.export_sql(OUTPUT)
    print(f"  ETL SQL files written to {OUTPUT}")

    print("Done.")


if __name__ == "__main__":
    main()
```

**Step 2: Run the integration script**

Run: `uv run python src/etl_main.py`
Expected: Should generate crosswalk.json, lineage files, and ETL SQL files in Sql_Commands/etl/

**Step 3: Verify output files exist**

```bash
ls -la Sql_Commands/etl/
cat Sql_Commands/etl/billing_etl.sql | head -30
```

**Step 4: Commit**

```bash
git add src/etl_main.py
git commit -m "feat: add ETL integration entry point with full workflow"
```

---

### Task 13: Run All Tests

**Step 1: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS (existing + new ETL tests)

**Step 2: Final commit if any adjustments needed**

```bash
git add -A
git commit -m "chore: final ETL module adjustments after full test run"
```
