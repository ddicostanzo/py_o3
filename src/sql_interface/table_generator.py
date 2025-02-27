from src.base.o3_key_element import O3KeyElement
from src.helpers.enums import SupportedSQLServers


class SQLKeyElementTableCreator:
    def __init__(self, sql_server_type: SupportedSQLServers, key_element: O3KeyElement):
        if sql_server_type not in SupportedSQLServers:
            raise KeyError(f"Provided SQL server {sql_server_type} is not supported. "
                           f"Only MSSQL and PSQL are supported.")

        self.sql_server_type = sql_server_type
        self.key_element = key_element

    def __sql_identity_field(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'{self.key_element.key_element_name}Id IDENTITY(1, 1) NOT NULL PRIMARY KEY'
        else:
            return f'{self.key_element.key_element_name}Id SERIAL PRIMARY KEY'

    def __sql_history_timestamp_field(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'HistoryDateTime datetime2 NOT NULL'
        else:
            return f'HistoryDateTime timestamptz NOT NULL'

    def __sql_history_user_field(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'HistoryUser varchar(max) NOT NULL'
        else:
            return f'HistoryUser text NOT NULL'

    @property
    def __create_table_prefix(self):
        return f'CREATE TABLE {self.key_element.key_element_name}'

    def create_sql_table_text(self, **kwargs):
        _fields = self.__sql_fields_for_table(**kwargs)
        _foreign_keys = self.__sql_foreign_keys_for_table(**kwargs)
        _text = f'{self.__create_table_prefix} ({", ".join(_fields)});'
        return _text

    def __sql_fields_for_table(self, **kwargs):
        _fields = [x.create_sql_field_text(self.sql_server_type, **kwargs) for x in self.key_element.list_attributes]
        _fields.insert(0, self.__sql_identity_field())
        _fields.append(self.__sql_history_timestamp_field())
        _fields.append(self.__sql_history_user_field())
        return _fields

    def __sql_foreign_keys_for_table(self, **kwargs):
        _foreign_keys = ""

        return _foreign_keys


class SQLStandardListTableCreator:
    def __init__(self, names):
        self.list_of_names = names


if __name__ == "__main__":
    pass
