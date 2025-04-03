from __future__ import annotations

from helpers.string_helpers import (leave_only_letters_numbers_or_underscore,
                                    leave_letters_numbers_spaces_underscores_dashes)
from src.helpers.enums import SupportedSQLServers
from sql_interface.data_model_to_sql.attribute_to_column import AttributeToSQLColumn
from sql_interface.data_model_to_sql.relationship_to_column import ChildRelationshipToColumn, InstanceRelationshipToColumn
from src.helpers.test_sql_server_type import check_sql_server_type

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.base.o3_key_element import O3KeyElement
    from base.o3_standard_value import O3StandardValue


class SQLTable:
    """
    The base SQL table class that contains the base properties
    and attributes to use to create the SQL command.
    """
    def __init__(self, sql_server_type: SupportedSQLServers):
        """
        Instantiates a new instance of the SQLTable class.

        Parameters
        ----------
        sql_server_type: SupportedSQLServers
            the SQL server type to use
        """
        if not check_sql_server_type(sql_server_type):
            raise Exception("Unsupported SQL Server Type")

        self.table_name = None
        self.columns = []

        self.sql_server_type = sql_server_type

    @property
    def table_prefix(self) -> str:
        """
        The table creation prefix which includes the command to create table
        with its table name

        Returns
        -------
            str
                the table creation prefix
        """
        return f'CREATE TABLE {self.table_name}'

    @property
    def table_suffix(self) -> str:
        """
        The table creation suffix. For MSSQL this will add the "WITH (SYSTEM VERSIONING ...);" command. For
        PSQL, it will return just a semicolon.

        Returns
        -------
            str
                the table creation suffix
        """
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            # return f''
            return f'WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.{self.table_name}History));\n'
        else:
            return ";"

    @property
    def identity_column(self) -> str:
        """
        Generates the identity column for the table using the table name and SQL type

        Returns
        -------
            str
                the identity column of the table as "{self.table_name}Id
        """
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'{self.table_name}Id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY'
        else:
            return f'{self.table_name}Id SERIAL PRIMARY KEY'

    @property
    def history_timestamp_column(self) -> str:
        """
        Generates the history timestamp column for the table using the SQL type

        Returns
        -------
            str
                for MSSQL, the columns ValidFrom, ValidTo, and PERIOD FOR SYSTEM_TIME are generated. For PSQL,
                a single column of "HistoryDateTime" is generated.
        """
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return (f'ValidFrom datetime2 GENERATED ALWAYS AS ROW Start,\n'
                    f'ValidTo datetime2 GENERATED ALWAYS AS ROW End,\n'
                    f'PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo)')
        else:
            return f'HistoryDateTime timestamptz DEFAULT CURRENT_TIMESTAMP'

    @property
    def history_user_column(self) -> str:
        """
        The history user column creation text to store the user who last modified the row.

        Returns
        -------
            str
                the column to store the last user who modified the data
        """
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return f'HistoryUser varchar(max) NOT NULL'
        else:
            return f'HistoryUser text NOT NULL'


class KeyElementTableCreator(SQLTable):
    """
    The class that creates a SQL table for an O3 Key Element.
    """
    def __init__(self, sql_server_type: SupportedSQLServers, key_element: "O3KeyElement"):
        """
        The instantiation of the Key Element Table Creator class that takes the Key Element and SQL type to use
        as the basis.

        Parameters
        ----------
        sql_server_type: SupportedSQLServers
            the SQL server type to use
        key_element: O3KeyElement
            the O3 Key Element to use
        """
        super().__init__(sql_server_type)

        self.key_element = key_element
        self.table_name = self.key_element.string_code.replace(' ', '')

    def _create_attribute_columns(self, phi_allowed: bool) -> None:
        """
        Creates instances of the Attribute to SQL Column class for each attribute in the
        key element.

        Parameters
        ----------
        phi_allowed: bool
            the flag to identify if the system is allowed to store PHI or not

        Returns
        -------
            None
        """
        for this_attr in self.key_element.list_attributes:
            self.columns.append(AttributeToSQLColumn(this_attr, phi_allowed, self.sql_server_type))

    def _create_foreign_key_columns(self) -> None:
        """
        Create instances of the Foreign Key Column class for each relationship in the
        key element.

        Returns
        -------
            None
        """
        for this_rel in self.key_element.child_of_relationships:
            _col_sql = ChildRelationshipToColumn(this_rel, self.sql_server_type)
            self.columns.append(_col_sql)

    def _create_instance_based_columns(self) -> None:
        """
        Currently unused, as instances are likely linked through Child elements. However, would create
        columns for the Instance based relationships if desired.

        Returns
        -------
            None
        """
        for this_rel in self.key_element.instance_of_relationships:
            _col_sql = InstanceRelationshipToColumn(this_rel, self.sql_server_type)
            self.columns.append(_col_sql)

    def _create_columns(self, phi_allowed: bool) -> None:
        """
        Create the columns for the given Key Element

        Parameters
        ----------
        phi_allowed : bool
            the flag to identify if the system is allowed to store PHI or not

        Returns
        -------
            None
        """
        self._create_foreign_key_columns()
        self._create_attribute_columns(phi_allowed)

        # Instance Of columns could have an intermediary table ActInstToProcCode style
        # self._create_instance_based_columns()

    def sql_table(self, phi_allowed: bool, **kwargs) -> str:
        """
        Using the data provided by the Key Element, Attributes, and Relationships,
        generates the text to allow the creation of the SQL table

        Parameters
        ----------
        phi_allowed: bool
            the flag to identify if the system is allowed to store PHI or not
        kwargs
            None currently configured

        Returns
        -------
            str
                the table creation text that contains all columns that should be stored
        """
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
    """
    Base class to create Standard List tables
    """
    def __init__(self, sql_server_type: SupportedSQLServers, title: str, items: list["O3StandardValue"]):
        """
        Instantiates a new Standard List Table Creator class used for generating a table for the standard value lists

        Parameters
        ----------
        sql_server_type: SupportedSQLServers
            the SQL server type to use
        title: str
            the table name
        items: list[O3StandardValue]
            the items to include in the table
        """
        super().__init__(sql_server_type)

        self.table_name = leave_only_letters_numbers_or_underscore(title)
        self.items = items

        if self.sql_server_type == SupportedSQLServers.MSSQL:
            self.key_element = "KeyElement varchar(256) NOT NULL"
            self.attribute = "Attribute varchar(256) NOT NULL"
            self.standard_value_item = "StandardValueItemName varchar(256) NOT NULL"
            self.numeric_code = "NumericCode varchar(32) NOT NULL"
            self.active_flag = "ActiveFlag bit NOT NULL DEFAULT 1"
            self.unique_constraint = "CONSTRAINT AK_NumericCode Unique(NumericCode)"
            self.index = (f"CREATE NONCLUSTERED INDEX IX_StandardValueLookup_NumericCode ON {self.table_name} "
                          f"(NumericCode) INCLUDE (KeyElement, Attribute);\n")
        else:
            self.key_element = "KeyElement text NOT NULL"
            self.attribute = "Attribute text NOT NULL"
            self.standard_value_item = "StandardValueItemName text NOT NULL"
            self.numeric_code = "NumericCode text NOT NULL"
            self.active_flag = "ActiveFlag boolean NOT NULL DEFAULT 1"
            self.unique_constraint = "Unique(NumericCode)"
            self.index = (f"CREATE INDEX idx_StandardValueLookup_NumericCode ON {self.table_name} "
                          f"(NumericCode) INCLUDE (KeyElement, Attribute);\n")

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

    def sql_table(self) -> str:
        """
        The SQL command text to generate the table on a server.

        Returns
        -------
            str
                the full SQL command text to generate the table for the standard value lists including inserting
                the appropriate values
        """
        _field_list = self.columns
        _joined_field_list = ", \n".join(_field_list)
        _text = f'{self.table_prefix} (\n{_joined_field_list}\n)\n{self.table_suffix}\n'
        _commands = self.insert_commands()
        for _command in _commands:
            _text += _command
        return _text

    def insert_commands(self) -> list[str]:
        """
        Generates the insert commands for the standard value list items.

        Returns
        -------
            list[str]
                the commands used to insert values into the table
        """
        _commands = [self.index]

        for x in self.items:
            _commands.append(f"INSERT INTO {self.table_name} (KeyElement, Attribute, StandardValueItemName, "
                             f"NumericCode, HistoryUser) "
                             f"VALUES ('{x.key_element.string_code}', '{x.attribute.string_code}', "
                             f"'{leave_letters_numbers_spaces_underscores_dashes(x.value_name)}', "
                             f"'{x.numeric_code}', 'db_creation');\n")

        _commands += "\n"
        return _commands


class LookupTableCreator(StandardListTableCreator):
    """
    An inherited class that generates a single lookup table for all standard value lists.
    """
    def __init__(self, sql_server_type: SupportedSQLServers, items: list["O3StandardValue"]):
        """
        Instantiates a new Lookup Table class used for generating a table for the standard value lists

        Parameters
        ----------
        sql_server_type: SupportedSQLServers
            the SQL server type to use
        items: list[O3StandardValue]
            the items to include in the table
        """
        super().__init__(sql_server_type, "StandardValuesLookup", items)


if __name__ == "__main__":
    pass
