from helpers.enums import SupportedSQLServers


def identity_column(sql_server_type, key_element):
    if sql_server_type == SupportedSQLServers.MSSQL:
        return f'{key_element.key_element_name}Id IDENTITY(1, 1) NOT NULL PRIMARY KEY'
    else:
        return f'{key_element.key_element_name}Id SERIAL PRIMARY KEY'


def history_timestamp_column(sql_server_type):
    if sql_server_type == SupportedSQLServers.MSSQL:
        return f'HistoryDateTime datetime2 NOT NULL'
    else:
        return f'HistoryDateTime timestamptz NOT NULL'


def history_user_column(sql_server_type):
    if sql_server_type == SupportedSQLServers.MSSQL:
        return f'HistoryUser varchar(max) NOT NULL'
    else:
        return f'HistoryUser text NOT NULL'


def __sql_fields_for_table(sql_server_type, key_element, **kwargs):
    _fields = [attribute_to_column(sql_server_type, x, **kwargs) for x in key_element.list_attributes]
    _fields.insert(0, identity_column(sql_server_type, key_element))
    _fields.append(history_timestamp_column(sql_server_type))
    _fields.append(history_user_column(sql_server_type))
    return _fields


if __name__ == "__main__":
        pass
