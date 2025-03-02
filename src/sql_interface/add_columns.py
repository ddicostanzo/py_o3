from src.helpers.test_sql_server_type import check_sql_server_type


def add_column_sql_command(table, column_name, column_type, sql_server_type):
    if not check_sql_server_type(sql_server_type):
        raise Exception("Unsupported SQL Server Type")




if __name__ == "__main__":
    pass