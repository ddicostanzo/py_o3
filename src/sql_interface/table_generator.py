from src.base.o3_key_element import O3KeyElement
from src.helpers.enums import SupportedSQLServers
from src.sql_interface.attribute_to_column import AttributeToSQLColumn


class KeyElementTableCreator:
    def __init__(self, sql_server_type: SupportedSQLServers, key_element: O3KeyElement):
        if sql_server_type not in SupportedSQLServers:
            raise KeyError(f"Provided SQL server {sql_server_type} is not supported. "
                           f"Only MSSQL and PSQL are supported.")

        self.sql_server_type = sql_server_type
        self.key_element = key_element
        self.table_name = self.key_element.key_element_name.replace(' ', '')
        self.columns = []

    @property
    def __table_prefix(self):
        return f'CREATE TABLE {self.table_name}'

    @property
    def identity_column(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'{self.table_name}Id IDENTITY(1, 1) NOT NULL PRIMARY KEY'
        else:
            return f'{self.table_name}Id SERIAL PRIMARY KEY'

    @property
    def history_timestamp_column(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'HistoryDateTime datetime2 NOT NULL'
        else:
            return f'HistoryDateTime timestamptz NOT NULL'

    @property
    def history_user_column(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'HistoryUser varchar(max) NOT NULL'
        else:
            return f'HistoryUser text NOT NULL'

    def _create_columns(self, phi_allowed):
        for this_attr in self.key_element.list_attributes:
            self.columns.append(AttributeToSQLColumn(this_attr, phi_allowed, self.sql_server_type))

    def sql_table(self, phi_allowed, **kwargs):
        if len(self.columns) == 0:
            self._create_columns(phi_allowed)

        _column_sql_text = [x.column_creation_text for x in self.columns]
        _column_sql_text.insert(0, self.identity_column)
        _column_sql_text.append(self.history_timestamp_column)
        _column_sql_text.append(self.history_user_column)

        _foreign_keys = self.__foreign_keys_for_table(**kwargs)
        _field_list = _column_sql_text + _foreign_keys
        _text = f'{self.__table_prefix} ({", ".join(_field_list)});'
        return _text

    def __foreign_keys_for_table(self, **kwargs) -> list:
        _foreign_keys = []

        return _foreign_keys


class StandardListTableCreator:
    def __init__(self, names):
        self.list_of_names = names


if __name__ == "__main__":
    pass
