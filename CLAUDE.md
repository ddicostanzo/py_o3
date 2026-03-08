# CLAUDE.md - py_o3

## Project Overview
A Python library to parse the O3 (Ontology for Oncology) data model from its JSON schema and generate SQL database structures. The JSON schema is downloaded from the [AAPM BDSC website](https://aapmbdsc.azurewebsites.net/Home/Downloads). Supports both MSSQL and PostgreSQL output.

## Tech Stack
- **Python 3.10+** with **hatchling** build system
- **uv** for package management (`uv.lock` tracked)
- **Dependencies**: `pyodbc` (DB connectivity), `python-dotenv` (env config from `.env` files)
- **Testing**: `pytest` (dev dependency), tests use `unittest.mock.MagicMock` extensively
- No linter/formatter configured

## Commands
```bash
uv run pytest                     # Run all tests
uv run pytest tests/<file>.py     # Run specific test file
uv run pytest -v                  # Verbose test output
uv run python src/main.py         # Run main script (requires .env with DB config)
uv sync --dev                     # Install all dependencies
```

## Project Structure
```
src/
  api/
    data_model.py              # O3DataModel class — main entry point
  base/
    o3_element.py              # O3Element — base class for all O3 entities
    o3_key_element.py          # O3KeyElement — top-level ontology element (extends O3Element)
    o3_attribute.py            # O3Attribute — attribute of a key element (extends O3Element)
    o3_relationship.py         # O3Relationship — links between key elements
    o3_standard_value.py       # O3StandardValue — enumerated allowed values for attributes
  helpers/
    string_helpers.py          # Regex sanitization functions for SQL identifiers
    enums.py                   # SupportedSQLServers, SQLColumnDataTypes, ServerToConnect, SQLAuthentication
    test_sql_server_type.py    # Validation function: check_sql_server_type()
  sql/
    connection/
      mssql.py                 # MSSQLConnection — pyodbc connection via .env config
    data_model_to_sql/
      table_generator.py       # SQLTable, KeyElementTableCreator, CustomTable, StandardListTableCreator,
                               #   LookupTableCreator, PatientIdentifierHash
      attribute_to_column.py   # AttributeToSQLColumn — maps O3 attributes to SQL columns
      relationship_to_column.py # ChildRelationshipToColumn, InstanceRelationshipToColumn
      foreign_keys.py          # ForeignKeysConstraints — ALTER TABLE FK commands
      add_columns.py           # add_column_sql_command(), add_foreign_key_column_sql_command()
      sql_type_from_o3_data_type.py  # sql_data_types dict mapping O3 types → SQL types per server
    aria_integration/
      queried_datatable.py     # Datatable — base class for parameterized SQL queries via pyodbc
      patient.py               # Patient — queries Aria DWH for patient data
    queries/Aura/
      patient.sql              # Patient demographics from Aria DWH
      patient_information.sql  # Patient address/details query (parameterized @mrn)
  Resources/
    O3_20250128_Fixed.json     # Latest fixed O3 schema
    O3_20250128.json           # O3 schema (Jan 2025)
    O3_20250119.json           # O3 schema (Jan 2025, earlier)
    O3_20240918.json           # O3 schema (Sep 2024)
    cpt_code_list_2025.txt     # CPT code reference data
  main.py                      # Script entry point — full workflow demo
tests/
  conftest.py                  # Adds src/ to sys.path
  test_string_helpers.py       # Tests for sanitization functions
  test_check_sql_server_type.py # Tests for server type validation
  test_sql_type_mapping.py     # Tests for O3→SQL type mapping (MSSQL + PSQL)
  test_o3_attribute.py         # Tests for attribute cleaning logic
  test_o3_standard_value.py    # Tests for standard value parsing
  test_add_columns.py          # Tests for ALTER TABLE column commands
  test_table_generator.py      # Tests for table creation classes
Sql_Commands/                  # Generated SQL output (gitignored)
```

## Architecture & Data Flow

### Parsing Pipeline
1. `O3DataModel.__init__(json_file)` reads the JSON schema file
2. `__json_to_dictionary()` parses JSON, fixing Unicode escape issues (`\\u002B`)
3. `__create_key_elements()` iterates the JSON array, creating `O3KeyElement` objects
4. Each `O3KeyElement` creates its `O3Attribute` list and `O3Relationship` list from nested dicts
5. Each `O3Attribute` creates its `O3StandardValue` list from `StandardValuesList` strings
6. Optional `clean=True` flag triggers data normalization on attributes

### Class Hierarchy
```
O3Element (base: value_name, value_type, string_code, numeric_code, definition, ...)
├── O3KeyElement (key_element_name, is_longitudinal, list_attributes, relationships)
└── O3Attribute (key_element ref, value_data_type, standard_values_list, allow_null_values)

O3Relationship (subject_element, relationship_category, predicate_element, cardinality)
O3StandardValue (key_element ref, attribute ref, value_name, numeric_code)

SQLTable (base: table_name, columns, sql_server_type)
├── KeyElementTableCreator (creates table from O3KeyElement)
└── CustomTable (creates table from static column definitions)
    ├── StandardListTableCreator (standard value list tables + INSERT commands)
    │   └── LookupTableCreator (single combined lookup table)
    └── PatientIdentifierHash (MRN ↔ anonymous ID mapping table)
```

### SQL Generation
- `KeyElementTableCreator` maps each `O3KeyElement` → CREATE TABLE with:
  - Identity column (INT IDENTITY for MSSQL, SERIAL for PSQL)
  - Attribute columns via `AttributeToSQLColumn` (data type mapping chain)
  - Foreign key columns via `ChildRelationshipToColumn` (from "ChildElement-Of" relationships)
  - History columns (MSSQL: temporal tables with SYSTEM_VERSIONING; PSQL: timestamp column)
- `ForeignKeysConstraints` generates ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY commands
- `StandardListTableCreator` / `LookupTableCreator` create tables for standard value enumerations with INSERT commands
- `PatientIdentifierHash` creates MRN→AnonPatID mapping table with FK to Patient

### Key Design Patterns
- **Data type normalization chain**: `AttributeToSQLColumn` uses a priority-ordered list of `__set_*` methods; first match wins
- **PHI-aware column generation**: Nullable logic flips based on `phi_allowed` flag (e.g., MRN is NOT NULL when PHI allowed, NULL otherwise)
- **Lazy properties**: `O3DataModel` uses `@property` with private cache (`__standard_value_lists`, etc.)
- **Cached properties**: `O3KeyElement.child_of_relationships` uses `@cached_property`
- **Dual SQL dialect support**: All SQL generators branch on `SupportedSQLServers.MSSQL` vs `.PSQL`

### Attribute Cleaning (`clean=True`)
When enabled, `O3Attribute` performs three cleaning steps:
1. **`__check_reference_system`**: Extracts reference system (e.g., "ICD-10") from standard values list when `ReferenceSystemForValues` is None
2. **`__clean_standard_values_list`**: Removes "Reference System:" and "Current ICD standard" entries from standard values
3. **`__clean_value_data_types`**: Normalizes data types ("Int"→"Integer", "Numeric"→"Decimal", "string"→"String", date inference from value name)

### Database Connectivity
- `MSSQLConnection` reads config from `.env` file with prefixed keys (e.g., `O3_SERVER`, `AURA_DATABASE`)
- Supports SQL auth (UID/PWD) and Windows Integrated auth
- `ServerToConnect` enum selects O3 or Aura database
- `Datatable` base class provides generator-based and batch query execution via pyodbc

## Code Style
- **snake_case** for functions, methods, variables, file names
- **PascalCase** for classes (e.g., `O3DataModel`, `SQLTable`)
- **Double underscore** prefix for private methods (`__json_to_dictionary`, `__set_nullable`)
- **Single underscore** prefix for protected methods (`_create_columns`, `_get_data`)
- **Type hints** on parameters and instance variables; uses Python 3.10+ built-in generics (`list[str]`, `dict[str, ...]`)
- **Forward references** as strings (`"O3KeyElement"`)
- **Docstrings**: NumPy-style (`Parameters`, `Returns` sections) for classes and main API; Google-style (`Args`, `Returns`) for simple helpers
- **`if __name__ == "__main__": pass`** guard in most modules
- Tests use class-based grouping (e.g., `TestCleanStandardValuesList`) with descriptive method names
- Test helpers use `_make_*` factory functions with `MagicMock` for parent objects

## MCP Tools Available

### Serena (Semantic Code Intelligence)
Serena provides LSP-powered symbolic code analysis and editing for this project.

**Navigation & Search:**
- `get_symbols_overview` - High-level view of symbols in a file (with configurable depth)
- `find_symbol` - Find symbols by name path pattern (supports substring matching, depth, body inclusion)
- `find_referencing_symbols` - Find all references to a symbol with surrounding context
- `search_for_pattern` - Regex search across codebase with file filtering (glob, code-only)
- `find_file` - Find files by name/mask
- `list_dir` - List directory contents (recursive option)

**Editing (Symbol-Level):**
- `replace_symbol_body` - Replace entire symbol definition
- `insert_after_symbol` / `insert_before_symbol` - Insert code relative to symbols
- `rename_symbol` - Rename a symbol across the entire codebase

**Editing (Line-Level):**
- `replace_lines` / `delete_lines` / `insert_at_line` - Line-based edits

**Memory & State:**
- `write_memory` / `read_memory` / `edit_memory` / `list_memories` / `delete_memory` / `rename_memory`
- `switch_modes` - Change modes (editing, planning, interactive, one-shot, onboarding)
- `query_project` / `list_queryable_projects` - Query other configured projects

**Workflow:**
- `think_about_collected_information` - Reflect on gathered data after search sequences
- `think_about_task_adherence` - Check alignment before making edits
- `think_about_whether_you_are_done` - Verify task completion
- `summarize_changes` - Summarize edits made to the codebase
- `restart_language_server` - Reset LSP if it hangs

### GrepAI (Semantic Code Search)
GrepAI provides AI-powered semantic search using local embeddings (nomic-embed-text via Ollama).
**Note**: py_o3 is not currently in GrepAI's index. The index covers other projects in the workspace (8,940 chunks, 1,478 files). GrepAI tools will return results from other indexed projects, not py_o3.

**Search:**
- `grepai_search` - Natural language semantic code search with workspace/project filtering
- `grepai_rpg_search` - Search RPG nodes semantically (RPG not enabled)

**Call Graph Tracing:**
- `grepai_trace_callers` - Find all callers of a symbol
- `grepai_trace_callees` - Find all callees of a symbol
- `grepai_trace_graph` - Build complete call graph around a symbol

**RPG Graph Exploration (not enabled):**
- `grepai_rpg_explore` - BFS traversal from a node
- `grepai_rpg_fetch` - Fetch detailed RPG node info

**Index Management:**
- `grepai_index_status` - Check index health (55.7 MB, last updated 2026-03-04)
- `grepai_list_workspaces` / `grepai_list_projects` - Discover workspaces and projects
