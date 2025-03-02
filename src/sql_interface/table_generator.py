from src.base.o3_key_element import O3KeyElement
from src.helpers.enums import SupportedSQLServers
from src.sql_interface.attribute_to_column import AttributeToSQLColumn
from src.helpers.test_sql_server_type import check_sql_server_type


class SQLTable:
    def __init__(self, sql_server_type):
        if not check_sql_server_type(sql_server_type):
            raise Exception("Unsupported SQL Server Type")

        self.table_name = None
        self.columns = []

        self.sql_server_type = sql_server_type

    @property
    def table_prefix(self):
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
            return f'HistoryDateTime datetime2 NOT NULL DEFAULT SYSDATETIMEOFFSET()'
        else:
            return f'HistoryDateTime timestamptz DEFAULT CURRENT_TIMESTAMP'

    @property
    def history_user_column(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'HistoryUser varchar(max) NOT NULL'
        else:
            return f'HistoryUser text NOT NULL'


class KeyElementTableCreator(SQLTable):
    def __init__(self, sql_server_type: SupportedSQLServers, key_element: O3KeyElement):
        super().__init__(sql_server_type)

        self.key_element = key_element
        self.table_name = self.key_element.string_code.replace(' ', '')

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
        _text = f'{self.table_prefix} ({", ".join(_field_list)});'
        return _text


class StandardListTableCreator(SQLTable):
    def __init__(self, sql_server_type, title, items: list):
        super().__init__(sql_server_type)

        self.table_name = title.replace(' ', '')
        self.items = items

        self.standard_value_item = ""
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            self.standard_value_item = "StandardValueItemName varchar(max) NOT NULL"
        else:
            self.standard_value_item = "StandardValueItemName text NOT NULL"

        self.numeric_code = ""
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            self.numeric_code = "NumericCode varchar(max) NOT NULL"
        else:
            self.numeric_code = "NumericCode text NOT NULL"

        self.active_flag = ""
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            self.active_flag = "ActiveFlag bit NOT NULL DEFAULT 1"
        else:
            self.active_flag = "ActiveFlag boolean NOT NULL DEFAULT 1"

        self.columns = [self.identity_column,
                        self.standard_value_item,
                        self.numeric_code,
                        self.active_flag,
                        self.history_timestamp_column,
                        self.history_user_column]

    def sql_table(self):
        _field_list = self.columns
        _text = f'{self.table_prefix} ({", ".join(_field_list)});'
        _commands = self.insert_commands()
        for _command in _commands:
            _text += _command
        return _text

    def insert_commands(self):
        _commands = []

        for x in self.items:
            _commands.append(f"INSERT INTO {self.table_name} (StandardValueItemName, NumericCode,"
                             f"HistoryUser) "
                             f"VALUES ('{x.value_name}', '{x.numeric_code}', 'db_build');")

        return _commands


if __name__ == "__main__":
    pass
