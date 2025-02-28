from src.base.o3_key_element import O3KeyElement
from src.helpers.enums import SupportedSQLServers


class KeyElementTableCreator:
    def __init__(self, sql_server_type: SupportedSQLServers, key_element: O3KeyElement):
        if sql_server_type not in SupportedSQLServers:
            raise KeyError(f"Provided SQL server {sql_server_type} is not supported. "
                           f"Only MSSQL and PSQL are supported.")

        self.sql_server_type = sql_server_type
        self.key_element = key_element

    @property
    def __create_table_prefix(self):
        return f'CREATE TABLE {self.key_element.key_element_name}'

    def create_sql_table_text(self, **kwargs):
        _fields = self.__sql_fields_for_table(**kwargs)
        _foreign_keys = self.__sql_foreign_keys_for_table(**kwargs)
        _text = f'{self.__create_table_prefix} ({", ".join(_fields)});'
        return _text

    def __sql_foreign_keys_for_table(self, **kwargs):
        _foreign_keys = ""

        return _foreign_keys


class StandardListTableCreator:
    def __init__(self, names):
        self.list_of_names = names


if __name__ == "__main__":
    pass
