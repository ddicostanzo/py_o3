# py_o3

A Python library to parse the [O3 (Ontology for Oncology)](https://aapmbdsc.azurewebsites.net/Home/Downloads) data model from its JSON schema and generate SQL DDL statements for MSSQL and PostgreSQL databases.

## Installation

```bash
uv sync --dev
```

## Usage

### Parse the O3 JSON schema

```python
from api.data_model import O3DataModel

model = O3DataModel("Resources/O3_20250128.json", clean=True)

# Inspect key elements
for name, element in model.key_elements.items():
    print(name, len(element.list_attributes), "attributes")
```

### Generate SQL tables

```python
from api.workflow import create_model, create_tables, create_standard_value_lookup_table, foreign_key_constraints
from helpers.enums import SupportedSQLServers

model = create_model("Resources/O3_20250128.json", clean=True)
sql_type = SupportedSQLServers.MSSQL  # or SupportedSQLServers.PSQL

# Generate CREATE TABLE statements for all key elements
tables = create_tables(model, sql_type, phi_allowed=True)

# Generate a standard value lookup table with INSERT commands
lookup = create_standard_value_lookup_table(model, sql_type)
tables["StandardValueLookup"] = lookup.sql_table()
insert_commands = lookup.insert_commands()

# Generate ALTER TABLE foreign key constraints
fk_commands = foreign_key_constraints(model, sql_type)
```

### Write SQL to file

```python
from api.workflow import write_sql_to_text

write_sql_to_text("output.sql", [v for v in tables.values()], write_mode="w")
write_sql_to_text("output.sql", insert_commands)
write_sql_to_text("output.sql", fk_commands)
```

## Architecture

### Parsing pipeline

1. `O3DataModel.__init__(json_file)` reads the JSON schema file.
2. `__json_to_dictionary()` parses JSON, fixing Unicode escape issues (`\\u002B`).
3. `__create_key_elements()` creates `O3KeyElement` objects from the JSON array.
4. Each `O3KeyElement` creates its `O3Attribute` and `O3Relationship` lists.
5. Each `O3Attribute` creates its `O3StandardValue` list from `StandardValuesList` strings.
6. The optional `clean=True` flag normalizes data types and cleans standard value lists.

### SQL generation

- `KeyElementTableCreator` maps each `O3KeyElement` to a `CREATE TABLE` statement with identity, attribute, foreign key, and history columns.
- `AttributeToSQLColumn` maps O3 attributes to SQL columns using a priority-ordered data type resolution chain.
- `ForeignKeysConstraints` generates `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY` commands from `ChildElement-Of` relationships.
- `StandardListTableCreator` / `LookupTableCreator` create tables for standard value enumerations with batched `INSERT` commands.
- `PatientIdentifierHash` creates an MRN-hash-to-anonymous-ID mapping table.

### SQL dialect support

SQL generation supports both MSSQL and PostgreSQL via the `SQLDialect` protocol (`src/sql/dialect.py`) with concrete implementations in `src/sql/dialects/`. MSSQL uses temporal tables with `SYSTEM_VERSIONING`; PostgreSQL uses a simple timestamp column.

## Environment setup

Database connectivity (optional) requires a `.env` file with prefixed keys. See `.env.example` for the required format. Keys use the pattern `O3_SERVER`, `O3_DATABASE`, `AURA_SERVER`, etc.

## Running tests

```bash
uv run pytest           # Run all tests
uv run pytest -v        # Verbose output
uv run pytest tests/test_table_generator.py  # Run a specific test file
```

## Dependencies

- **Python 3.10+**
- `pyodbc` -- database connectivity (optional, only needed for Aria integration)
- `python-dotenv` -- `.env` file parsing
- `pytest` -- testing (dev dependency)

## License

See repository for license details.
