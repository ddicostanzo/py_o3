# ETL Mapping Design — DWH → O3

**Date:** 2026-03-08
**Status:** Approved

## Goals

1. **Crosswalk mapping** — Auto-suggest and curate column-level mappings from Varian ARIA DWH tables to O3 ontology attributes
2. **Data lineage** — Trace the flow from DWH source columns through model transformations to O3 targets, with coverage reporting
3. **ETL generation** — Produce executable Python + SQL pipelines that extract from DWH models and load into O3 tables, supporting both live execution and offline SQL export

## Source Files

| File | Purpose |
|---|---|
| `model_registry.json` | Entry points, join/time/field policies, query safety rules |
| `semantic_manifest_from_variandw_schema.json` | Raw DWH schema: 352 tables (40 fact, 96 dimension, 216 other), 470 FKs |
| `semantic_manifest_with_models.json` | 11 conceptual models with SELECT column mappings and expressions |

## Architecture: Layered Approach

Three independent layers, each consuming the one before it:

```
Mapping (crosswalk) → Lineage (documentation) → Pipeline (ETL execution)
```

All layers share two foundation loaders (`registry.py`, `manifest.py`) that parse the JSON resources into typed Python dataclasses.

## Layer 1: Data Foundation

### `registry.py` — Model Registry Loader

Parses `model_registry.json` into typed dataclasses:

- `ModelRegistry` — top-level container
  - `entry_points: dict[str, EntryPoint]` — billing, scheduling, activity, treatment_history
  - `models: dict[str, ModelConfig]` — per-model join policy, time policy, field policy
  - `global_policy` — timezone, date range modes, query safety rules
  - `field_policy_defaults: FieldPolicy` — global PHI deny list

Key types:
- `EntryPoint` — base_table, preferred_conceptual_model, time_policy
- `ModelConfig` — base_table, join_policy (allowed dimension joins), time_policy (date key candidates, date basis map), field_policy
- `TimePolicy` — default_date_key, date_basis (enum + map + default), default_lookback_days
- `JoinSpec` — table, from_column, to_column
- `QuerySafety` — select_only, row limits, require_date_filter_for_tables, cross_fact_joins policy

### `manifest.py` — Semantic Manifest Loader

Parses both manifest JSON files into:

- `SemanticManifest` — top-level container
  - `tables: dict[str, DWHTable]` — from variandw_schema (schema, name, type, columns, PKs, FKs)
  - `models: list[ConceptualModel]` — from manifest_with_models (name, base_table, tables_referenced, joins, selects)
  - `summary: dict`

Key types:
- `DWHTable` — schema, name, full_name, type (fact/dimension/other), columns, primary_key, foreign_keys
- `Column` — name, data_type, nullable, is_primary_key, is_foreign_key
- `ForeignKey` — from_column, to_table, to_column
- `ConceptualModel` — name, base_table, tables_referenced, joins, selects
- `ModelSelect` — alias, from_table, expr, data_type, tags

## Layer 2: Mapping (Crosswalk)

### `match_engine.py` — Scoring Logic

Compares DWH columns/model selects against O3 attributes using three signals:

1. **Name similarity** (highest weight) — token overlap after stripping prefixes (Dim, Fact, ID, Lookup)
2. **Type compatibility** — compatibility matrix between DWH types (VDT_*, SQL types) and O3 types (Boolean, String, Integer, etc.)
3. **Context bonus** — same semantic domain (patient, treatment, billing) boosts score

Confidence thresholds:
- ≥0.8: high confidence (auto-accepted in suggestions)
- 0.5–0.8: medium (flagged for review)
- <0.5: excluded

Output: `MatchCandidate` with composite score and signal breakdown.

### `crosswalk.py` — Orchestrator

`Crosswalk` class:
- `generate_suggestions()` — runs match engine across all DWH columns × O3 attributes, returns ranked `CrosswalkEntry` list
- `load_curated(path)` / `save_curated(entries, path)` — JSON persistence
- `merge(suggestions, curated)` — preserves human decisions (confirmed/rejected), adds new suggestions for unmatched items

`CrosswalkEntry` fields:
- `dwh_table`, `dwh_column` — source location
- `model_name`, `model_alias`, `model_expr` — conceptual model context (expr preserved as opaque text for lineage)
- `o3_key_element`, `o3_attribute` — target
- `confidence` — match score
- `status` — "auto" | "confirmed" | "rejected" | "manual"

### `mapping_store.py` — Persistence

Handles load/save of crosswalk JSON, plus `diff()` to show what changed between runs.

### Workflow

1. First run: `generate_suggestions()` → saves to `src/Resources/crosswalk.json`
2. Human reviews JSON, changes status values, adds manual entries
3. Subsequent runs: `merge()` preserves curated decisions, suggests only new unmatched items

## Layer 3: Lineage

### `lineage_builder.py` — Graph Construction

Builds a directed graph from confirmed crosswalk entries:

- **Source nodes** — DWH table + column
- **Transform nodes** — intermediate step when a model expression exists (expression text preserved as metadata)
- **Target nodes** — O3 key element + attribute
- **Edges** — connect nodes with model name and confidence

Traversal methods:
- `trace_forward(source_table, source_column)` — what O3 attributes does this DWH column feed?
- `trace_backward(o3_element, o3_attribute)` — what DWH columns feed this O3 attribute?
- `unmapped_sources()` — DWH columns with no O3 target
- `unmapped_targets()` — O3 attributes with no DWH source

### `lineage_report.py` — Export

Three output formats:
- **JSON** — machine-readable full graph
- **Markdown** — human-readable tables per O3 key element, plus unmapped lists
- **Coverage summary** — stats dict with total, mapped, unmapped counts and percentages, broken down by entry point

## Layer 4: Pipeline (ETL Execution)

### `extractor.py` — SELECT Generation

`Extractor.generate_query(entry_point, date_basis, lookback_days)` builds a SELECT statement:

1. Starts from entry point's base table
2. JOINs only dimensions referenced by mapped crosswalk entries (respects `allowedDimensionJoins`)
3. SELECT list uses model expressions for computed columns, direct references otherwise
4. WHERE applies date filter using time policy
5. Excludes PHI columns per field policy
6. Enforces query safety (row limit, select-only)

### `loader.py` — INSERT/MERGE Generation

`Loader` generates load SQL targeting O3 tables:
- `generate_insert(extract)` — simple INSERT
- `generate_merge(extract, merge_key)` — MERGE/upsert

Uses existing `KeyElementTableCreator` to know target table structure — no duplicate O3→SQL mapping.

### `runner.py` — Orchestration

`ETLRunner` supports two modes:
- **Offline**: `export_sql(output_dir)` writes SQL files to `Sql_Commands/etl/`
- **Live**: `run(entry_points, date_basis, lookback_days, dry_run)` executes via `MSSQLConnection`

Execution flow per entry point: generate extract → execute/export → generate load → execute/export.

Returns `ETLResult` with per-entry-point stats (rows extracted, rows loaded, duration, errors).

### Safety (from model registry)

- Date filters always applied to tables in `requireDateFilterForTables`
- Row limits enforced per `querySafety`
- Cross-fact joins blocked unless bridge table exists
- PHI columns excluded per `fieldPolicy`

## File Layout

```
src/
  etl/
    __init__.py
    registry.py
    manifest.py
    mapping/
      __init__.py
      match_engine.py
      crosswalk.py
      mapping_store.py
    lineage/
      __init__.py
      lineage_builder.py
      lineage_report.py
    pipeline/
      __init__.py
      extractor.py
      loader.py
      runner.py
  Resources/
    model_registry.json              (existing)
    semantic_manifest_from_variandw_schema.json  (existing)
    semantic_manifest_with_models.json           (existing)
    crosswalk.json                   (generated, then curated)
tests/
  etl/
    test_registry.py
    test_manifest.py
    test_match_engine.py
    test_crosswalk.py
    test_lineage_builder.py
    test_lineage_report.py
    test_extractor.py
    test_loader.py
    test_runner.py
```

## Dependency Graph

```
model_registry.json ──→ registry.py ──┐
                                      ├──→ crosswalk.py ──→ lineage_builder.py ──→ lineage_report.py
semantic_manifests ────→ manifest.py ──┤                          │
                                      ├──→ extractor.py ◄────────┘
O3 JSON schema ────→ O3DataModel ─────┤
                                      └──→ loader.py ───→ runner.py
                                                              │
                                      MSSQLConnection ────────┘ (live mode)
                                      Sql_Commands/etl/ ──────┘ (offline mode)
```

## Out of Scope (YAGNI)

- No scheduling/cron — run manually or integrate externally
- No incremental/CDC logic — full extract within lookback window
- No cross-fact joins — respects registry's `disallow_unless_bridge` rule
- No expression parsing — expressions are opaque, preserved as text for lineage
- No GUI for crosswalk review — edit JSON directly
- MSSQL only — no dual-dialect for ETL (O3 table generation retains dual-dialect support)

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Architecture | Layered (mapping → lineage → pipeline) | Matches existing project separation; independently testable |
| Crosswalk approach | Hybrid auto-suggest + human curation | Speed of automation with accuracy of human review |
| Expression handling | Opaque with text preserved | Avoids fragile SQL parsing; lineage still traceable by humans |
| ETL output | Python orchestration + raw SQL | Fits existing Datatable/MSSQLConnection patterns; SQL stays visible and auditable |
| SQL dialect | MSSQL only | DWH is ARIA/MSSQL; no need for PSQL ETL |
| O3 scope | Full ontology | All key elements mapped, not just entry-point models |
| Execution modes | Live + offline | Develop offline, run live; both supported by runner |
