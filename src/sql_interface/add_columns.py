from src.helpers.test_sql_server_type import check_sql_server_type
from src.sql_interface.sql_type_from_o3_data_type import sql_data_types

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.helpers.enums import SupportedSQLServers


def add_column_sql_command(table: str, column_name: str, column_type: str,
                           nullable: bool, sql_server_type: SupportedSQLServers) -> str:
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
        raise Exception("Unsupported SQL Server Type")

    _data_types = list(sql_data_types[sql_server_type].values())

    if column_type not in _data_types:
        raise TypeError(f"Column type {column_type} does not exist in specified types")

    _null = "NULL" if nullable else "NOT NULL"

    _statement = f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type} {_null};"

    return _statement


def add_foreign_key_column_sql_command(table: str, column_name: str, sql_server_type: SupportedSQLServers) -> str:
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
        raise Exception("Unsupported SQL Server Type")

    _int_type = ""
    if sql_server_type == "MSSQL":
        _int_type = "INT"
    else:
        _int_type = "INTEGER"

    return add_column_sql_command(table, column_name, _int_type, nullable=False, sql_server_type=sql_server_type)


if __name__ == "__main__":
    pass
