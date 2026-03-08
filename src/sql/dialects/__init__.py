"""Factory for SQL dialect instances based on server type."""
from __future__ import annotations

from functools import lru_cache

from helpers.enums import SupportedSQLServers
from helpers.validate_sql_server_type import check_sql_server_type
from sql.dialect import SQLDialect
from sql.dialects.mssql_dialect import MSSQLDialect
from sql.dialects.psql_dialect import PSQLDialect


@lru_cache(maxsize=2)
def get_dialect(server_type: SupportedSQLServers) -> SQLDialect:
    """
    Factory function to get the appropriate SQLDialect implementation
    for a given SQL server type. Results are cached — only one instance
    per dialect is ever created.

    Parameters
    ----------
    server_type : SupportedSQLServers
        the SQL server type enum value

    Returns
    -------
    SQLDialect
        the dialect implementation for the given server type

    Raises
    ------
    ValueError
        if the server type is not supported
    """
    check_sql_server_type(server_type)

    if server_type == SupportedSQLServers.MSSQL:
        return MSSQLDialect()
    elif server_type == SupportedSQLServers.PSQL:
        return PSQLDialect()
    raise ValueError(f"No dialect implementation for {server_type}")


if __name__ == "__main__":
    pass
