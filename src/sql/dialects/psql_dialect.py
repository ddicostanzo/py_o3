"""PostgreSQL dialect implementation for SQL type mappings and syntax."""
from __future__ import annotations


class PSQLDialect:
    """
    PostgreSQL dialect implementation providing type mappings and SQL syntax
    specific to PostgreSQL.
    """

    @property
    def name(self) -> str:
        return "PSQL"

    @property
    def type_map(self) -> dict[str, str]:
        return {
            "Boolean": "boolean",
            "Binary": "bytea",
            "Date": "timestamptz",
            "Decimal": "numeric(19,9)",
            "Integer": "integer",
            "String": "text",
        }

    @property
    def string_type(self) -> str:
        return "text"

    @property
    def integer_type(self) -> str:
        return "integer"

    @property
    def boolean_type(self) -> str:
        return "boolean"

    def identity_column(self, table_name: str) -> str:
        return f'{table_name}Id SERIAL PRIMARY KEY'

    def history_timestamp_columns(self) -> str:
        return 'HistoryDateTime timestamptz DEFAULT CURRENT_TIMESTAMP'

    def history_user_column(self) -> str:
        return 'HistoryUser text NOT NULL'

    def table_suffix(self, table_name: str) -> str:
        return ';'

    def string_type_short(self, max_length: int = 256) -> str:
        return "text"

    def unique_constraint(self, constraint_name: str, column: str) -> str:
        return f"Unique({column})"

    def create_index(self, index_name: str, table_name: str, column: str,
                     include_columns: list[str]) -> str:
        includes = ", ".join(include_columns)
        return (f"CREATE INDEX {index_name} ON {table_name} "
                f"({column}) INCLUDE ({includes});\n")

    @property
    def boolean_default_true(self) -> str:
        return "DEFAULT TRUE"

    @property
    def on_delete_restrict(self) -> str:
        return "RESTRICT"

    def alter_table_add_column(self, table: str, col_name: str, col_type: str, nullable: str) -> str:
        return f'ALTER TABLE {table} ADD COLUMN {col_name} {col_type} {nullable};'


if __name__ == "__main__":
    pass
