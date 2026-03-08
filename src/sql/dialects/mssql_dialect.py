"""MSSQL dialect implementation for SQL type mappings and syntax."""
from __future__ import annotations


class MSSQLDialect:
    """
    MSSQL dialect implementation providing type mappings and SQL syntax
    specific to Microsoft SQL Server.
    """

    @property
    def name(self) -> str:
        return "MSSQL"

    @property
    def type_map(self) -> dict[str, str]:
        return {
            "Boolean": "bit",
            "Binary": "varbinary",
            "Date": "datetime2",
            "Decimal": "decimal(19,9)",
            "Integer": "int",
            "String": "varchar(max)",
        }

    @property
    def string_type(self) -> str:
        return "varchar(max)"

    @property
    def integer_type(self) -> str:
        return "int"

    @property
    def boolean_type(self) -> str:
        return "bit"

    def identity_column(self, table_name: str) -> str:
        return f'{table_name}Id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY'

    def history_timestamp_columns(self) -> str:
        return ('ValidFrom datetime2 GENERATED ALWAYS AS ROW Start,\n'
                'ValidTo datetime2 GENERATED ALWAYS AS ROW End,\n'
                'PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo)')

    def history_user_column(self) -> str:
        return 'HistoryUser varchar(max) NOT NULL'

    def table_suffix(self, table_name: str) -> str:
        return f'WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.{table_name}History));\n'

    def string_type_short(self, max_length: int = 256) -> str:
        return f"varchar({max_length})"

    def unique_constraint(self, constraint_name: str, column: str) -> str:
        return f"CONSTRAINT {constraint_name} Unique({column})"

    def create_index(self, index_name: str, table_name: str, column: str,
                     include_columns: list[str]) -> str:
        includes = ", ".join(include_columns)
        return (f"CREATE NONCLUSTERED INDEX {index_name} ON {table_name} "
                f"({column}) INCLUDE ({includes});\n")

    def alter_table_add_column(self, table: str, col_name: str, col_type: str, nullable: str) -> str:
        return f'ALTER TABLE {table} ADD {col_name} {col_type} {nullable};'


if __name__ == "__main__":
    pass
