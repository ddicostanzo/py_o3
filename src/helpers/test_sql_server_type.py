from __future__ import annotations

from helpers.enums import SupportedSQLServers


def check_sql_server_type(sql_server_type: SupportedSQLServers) -> bool | ValueError:
    """
    Checks if the given SQL Server type is supported.

    Parameters
    ----------
    sql_server_type: SupportedSQLServers
        supplied SQL Server type

    Returns
    -------
        bool
            True if the given SQL Server type is supported, raises ValueError otherwise
    """
    if sql_server_type not in SupportedSQLServers:
        raise ValueError(f"Provided SQL server {sql_server_type} is not supported. "
                         f"Only MSSQL and PSQL are supported.")
    return True


if __name__ == "__main__":
    pass
