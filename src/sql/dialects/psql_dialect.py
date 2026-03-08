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

    def alter_table_add_column(self, table: str, col_name: str, col_type: str, nullable: str) -> str:
        return f'ALTER TABLE {table} ADD COLUMN {col_name} {col_type} {nullable};'


if __name__ == "__main__":
    pass
