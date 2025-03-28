from base.o3_standard_value import O3StandardValue
from helpers.string_helpers import clean_table_and_column_names, clean_text_values
from src.base.o3_key_element import O3KeyElement
from src.helpers.enums import SupportedSQLServers
from src.sql_interface.attribute_to_column import AttributeToSQLColumn
from src.sql_interface.relationship_to_column import ChildRelationshipToColumn, InstanceRelationshipToColumn
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
    def table_suffix(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            # return f''
            return f'WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.{self.table_name}History));'
        else:
            return ";"

    @property
    def identity_column(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'{self.table_name}Id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY'
        else:
            return f'{self.table_name}Id SERIAL PRIMARY KEY'

    @property
    def history_timestamp_column(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return (f'ValidFrom datetime2 GENERATED ALWAYS AS ROW Start,\n'
                    f'ValidTo datetime2 GENERATED ALWAYS AS ROW End,\n'
                    f'PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo)')
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

    def _create_attribute_columns(self, phi_allowed):
        for this_attr in self.key_element.list_attributes:
            self.columns.append(AttributeToSQLColumn(this_attr, phi_allowed, self.sql_server_type))

    def _create_foreign_key_columns(self):
        for this_rel in self.key_element.child_of_relationships:
            _col_sql = ChildRelationshipToColumn(this_rel, self.sql_server_type)
            self.columns.append(_col_sql)

    def _create_instance_based_columns(self):
        for this_rel in self.key_element.instance_of_relationships:
            _col_sql = InstanceRelationshipToColumn(this_rel, self.sql_server_type)
            self.columns.append(_col_sql)

    def _create_columns(self, phi_allowed):
        self._create_foreign_key_columns()
        self._create_attribute_columns(phi_allowed)

        # Instance Of columns should have an intermediary table ActInstToProcCode style
        # self._create_instance_based_columns()

    def sql_table(self, phi_allowed, **kwargs):
        if len(self.columns) == 0:
            self._create_columns(phi_allowed)

        _column_sql_text = [x.column_creation_text for x in self.columns]
        _column_sql_text.insert(0, self.identity_column)
        _column_sql_text.append(self.history_user_column)
        _column_sql_text.append(self.history_timestamp_column)
        _joined_field_list = ",\n".join(_column_sql_text)
        _text = f'{self.table_prefix} (\n{_joined_field_list}\n)\n{self.table_suffix}\n'
        return _text


class StandardListTableCreator(SQLTable):
    def __init__(self, sql_server_type, title, items: list[O3StandardValue]):
        super().__init__(sql_server_type)

        self.table_name = clean_table_and_column_names(title)
        self.items = items

        if self.sql_server_type == SupportedSQLServers.MSSQL:
            self.key_element = "KeyElement varchar(max) NOT NULL"
            self.attribute = "Attribute varchar(max) NOT NULL"
            self.standard_value_item = "StandardValueItemName varchar(max) NOT NULL"
            self.numeric_code = "NumericCode varchar(max) NOT NULL"
            self.active_flag = "ActiveFlag bit NOT NULL DEFAULT 1"
            self.unique_constraint = "CONSTRAINT AK_NumericCode Unique(NumericCode)"
        else:
            self.key_element = "KeyElement text NOT NULL"
            self.attribute = "Attribute text NOT NULL"
            self.standard_value_item = "StandardValueItemName text NOT NULL"
            self.numeric_code = "NumericCode text NOT NULL"
            self.active_flag = "ActiveFlag boolean NOT NULL DEFAULT 1"
            self.unique_constraint = "Unique(NumericCode)"

        self.columns = [self.identity_column,
                        self.key_element,
                        self.attribute,
                        self.standard_value_item,
                        self.numeric_code,
                        self.active_flag,
                        self.history_user_column,
                        self.history_timestamp_column,
                        self.unique_constraint
                        ]

    def sql_table(self):
        _field_list = self.columns
        _joined_field_list = ", \n".join(_field_list)
        _text = f'{self.table_prefix} (\n{_joined_field_list}\n)\n{self.table_suffix}\n'
        _commands = self.insert_commands()
        for _command in _commands:
            _text += _command
        return _text

    def insert_commands(self):
        _commands = []

        for x in self.items:
            _commands.append(f"INSERT INTO {self.table_name} (KeyElement, Attribute, StandardValueItemName, "
                             f"NumericCode, HistoryUser) "
                             f"VALUES ('{x.key_element.string_code}', '{x.attribute.string_code}', "
                             f"{clean_text_values(x.value_name)}', '{x.numeric_code}', 'db_creation');\n")

        return _commands


class LookupTableCreator(StandardListTableCreator):
    def __init__(self, sql_server_type, items: list[O3StandardValue]):
        super().__init__(sql_server_type, "StandardValuesLookup", items)


if __name__ == "__main__":
    pass
