"""Functions to generate ALTER TABLE ADD COLUMN SQL commands."""
from __future__ import annotations
from helpers.enums import SupportedSQLServers
from helpers.string_helpers import leave_only_letters_numbers_or_underscore
from helpers.validate_sql_server_type import check_sql_server_type
from sql.data_model_to_sql.sql_type_from_o3_data_type import sql_data_types
from sql.dialects import get_dialect


def add_column_sql_command(table: str, column_name: str, column_type: str,
                           nullable: bool, sql_server_type: "SupportedSQLServers") -> str:
    """
    Creates the SQL command to add a column to a table.

    Parameters
    ----------
    table: str
        the table name
    column_name: str
        the column name
    column_type: str
        the column type
    nullable: bool
        whether the column is nullable
    sql_server_type: SupportedSQLServers
        the SQL server type

    Returns
    -------
        str
            the alter table command to add a column to a table
    """
    if not check_sql_server_type(sql_server_type):
        raise ValueError("Unsupported SQL Server Type")

    _data_types = list(sql_data_types[sql_server_type].values())

    if column_type not in _data_types:
        raise TypeError(f"Column type {column_type} does not exist in specified types")

    _null = "NULL" if nullable else "NOT NULL"

    table = leave_only_letters_numbers_or_underscore(table)
    column_name = leave_only_letters_numbers_or_underscore(column_name)

    dialect = get_dialect(sql_server_type)
    return dialect.alter_table_add_column(table, column_name, column_type, _null)


def add_foreign_key_column_sql_command(table: str, column_name: str, sql_server_type: "SupportedSQLServers") -> str:
    """
    Creates the SQL command to add a foreign key column to a table.

    Parameters
    ----------
    table: str
        the table name to add a foreign key column
    column_name: str
        the column name
    sql_server_type: SupportedSQLServers
        the SQL server type

    Returns
    -------
        str
            the alter table command to add a foreign key column to a table
    """
    if not check_sql_server_type(sql_server_type):
        raise ValueError("Unsupported SQL Server Type")

    dialect = get_dialect(sql_server_type)
    _int_type = dialect.integer_type

    return add_column_sql_command(table, column_name, _int_type, nullable=False, sql_server_type=sql_server_type)


if __name__ == "__main__":
    pass
