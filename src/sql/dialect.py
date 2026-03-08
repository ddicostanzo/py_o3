"""Protocol definitions for SQL dialect and column generator interfaces."""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SQLDialect(Protocol):
    """
    Protocol defining the interface for SQL dialect implementations.

    Each dialect provides type mappings, column definitions, and SQL syntax
    specific to a particular SQL server type (e.g., MSSQL, PostgreSQL).
    """

    @property
    def name(self) -> str:
        """The name of the SQL dialect (e.g., 'MSSQL', 'PSQL')."""
        ...

    @property
    def type_map(self) -> dict[str, str]:
        """Mapping of O3 data types to SQL column types for this dialect."""
        ...

    @property
    def string_type(self) -> str:
        """The SQL string type for this dialect (e.g., 'varchar(max)', 'text')."""
        ...

    @property
    def integer_type(self) -> str:
        """The SQL integer type for this dialect (e.g., 'int', 'integer')."""
        ...

    @property
    def boolean_type(self) -> str:
        """The SQL boolean type for this dialect (e.g., 'bit', 'boolean')."""
        ...

    def identity_column(self, table_name: str) -> str:
        """
        Generate the identity/primary key column definition for a table.

        Parameters
        ----------
        table_name : str
            the name of the table

        Returns
        -------
        str
            the identity column definition
        """
        ...

    def history_timestamp_columns(self) -> str:
        """
        Generate the history timestamp column(s) definition.

        Returns
        -------
        str
            the history timestamp column(s) definition
        """
        ...

    def history_user_column(self) -> str:
        """
        Generate the history user column definition.

        Returns
        -------
        str
            the history user column definition
        """
        ...

    def table_suffix(self, table_name: str) -> str:
        """
        Generate the table creation suffix.

        Parameters
        ----------
        table_name : str
            the name of the table

        Returns
        -------
        str
            the table creation suffix
        """
        ...

    def string_type_short(self, max_length: int = 256) -> str:
        """
        A bounded string type for columns with known maximum lengths.

        For MSSQL this returns varchar(max_length), for PSQL this returns text
        (PostgreSQL does not benefit from bounded varchar).

        Parameters
        ----------
        max_length : int
            the maximum character length (used by MSSQL, ignored by PSQL)

        Returns
        -------
        str
            the bounded string type
        """
        ...

    def unique_constraint(self, constraint_name: str, column: str) -> str:
        """
        Generate a UNIQUE constraint clause for a CREATE TABLE statement.

        Parameters
        ----------
        constraint_name : str
            the constraint name (used by MSSQL, ignored by PSQL)
        column : str
            the column to constrain

        Returns
        -------
        str
            the unique constraint clause
        """
        ...

    def create_index(self, index_name: str, table_name: str, column: str,
                     include_columns: list[str]) -> str:
        """
        Generate a CREATE INDEX statement.

        Parameters
        ----------
        index_name : str
            the index name
        table_name : str
            the table to index
        column : str
            the indexed column
        include_columns : list[str]
            columns to include in the index

        Returns
        -------
        str
            the CREATE INDEX statement
        """
        ...

    def alter_table_add_column(self, table: str, col_name: str, col_type: str, nullable: str) -> str:
        """
        Generate an ALTER TABLE ADD COLUMN statement.

        Parameters
        ----------
        table : str
            the table name
        col_name : str
            the column name
        col_type : str
            the column type
        nullable : str
            the nullable constraint (e.g., 'NULL', 'NOT NULL')

        Returns
        -------
        str
            the ALTER TABLE statement
        """
        ...


@runtime_checkable
class ColumnGenerator(Protocol):
    """
    Protocol for objects that generate SQL column creation text.

    Satisfied by AttributeToSQLColumn, RelationshipToColumn, and similar
    column-generating classes.
    """

    @property
    def column_creation_text(self) -> str:
        """The SQL column definition text."""
        ...


if __name__ == "__main__":
    pass
