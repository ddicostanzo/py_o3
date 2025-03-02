from src.helpers.enums import SupportedSQLServers


def check_sql_server_type(sql_server_type):
    if sql_server_type not in SupportedSQLServers:
        raise KeyError(f"Provided SQL server {sql_server_type} is not supported. "
                       f"Only MSSQL and PSQL are supported.")
    return True


if __name__ == "__main__":
    pass
