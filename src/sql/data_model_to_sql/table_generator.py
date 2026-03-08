"""SQL table generators for O3 key elements, standard value lists, and custom tables."""
from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.enums import SupportedSQLServers
from helpers.string_helpers import (
    leave_letters_numbers_spaces_underscores_dashes,
    leave_only_letters_numbers_or_underscore,
)
from helpers.validate_sql_server_type import check_sql_server_type
from sql.data_model_to_sql.attribute_to_column import AttributeToSQLColumn
from sql.data_model_to_sql.relationship_to_column import ChildRelationshipToColumn, InstanceRelationshipToColumn
from sql.dialect import SQLDialect
from sql.dialects import get_dialect

if TYPE_CHECKING:
    from base.o3_key_element import O3KeyElement
    from base.o3_standard_value import O3StandardValue


class SQLTable:
    """
    The base SQL table class that contains the base properties
    and attributes to use to create the SQL command.
    """

    def __init__(self, sql_server_type: SupportedSQLServers | SQLDialect):
        """
        Instantiates a new instance of the SQLTable class.

        Parameters
        ----------
        sql_server_type: SupportedSQLServers | SQLDialect
            the SQL server type to use, or a pre-built SQLDialect instance
        """

        if isinstance(sql_server_type, SQLDialect):
            self.dialect = sql_server_type
            self.sql_server_type = (SupportedSQLServers.MSSQL
                                    if self.dialect.name == "MSSQL"
                                    else SupportedSQLServers.PSQL)
        else:
            if not check_sql_server_type(sql_server_type):
                raise ValueError("Unsupported SQL Server Type")
            self.sql_server_type = sql_server_type
            self.dialect = get_dialect(sql_server_type)

        self.table_name = None
        self.columns = []

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
        return self.dialect.table_suffix(self.table_name)

    @property
    def identity_column(self) -> str:
        """
        Generates the identity column for the table using the table name and SQL type

        Returns
        -------
            str
                the identity column of the table as "{self.table_name}Id
        """
        return self.dialect.identity_column(self.table_name)

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
        return self.dialect.history_timestamp_columns()

    @property
    def history_user_column(self) -> str:
        """
        The history user column creation text to store the user who last modified the row.

        Returns
        -------
            str
                the column to store the last user who modified the data
        """
        return self.dialect.history_user_column()


class KeyElementTableCreator(SQLTable):
    """
    The class that creates a SQL table for an O3 Key Element.
    """

    def __init__(self, sql_server_type: SupportedSQLServers, key_element: O3KeyElement,
                 phi_allowed: bool = False):
        """
        The instantiation of the Key Element Table Creator class that takes the Key Element and SQL type to use
        as the basis.

        Parameters
        ----------
        sql_server_type: SupportedSQLServers
            the SQL server type to use
        key_element: O3KeyElement
            the O3 Key Element to use
        phi_allowed: bool
            the flag to identify if the system is allowed to store PHI or not
        """
        super().__init__(sql_server_type)

        self.key_element = key_element
        self.phi_allowed = phi_allowed
        self.table_name = leave_only_letters_numbers_or_underscore(self.key_element.string_code)

    def _create_attribute_columns(self) -> None:
        """
        Creates instances of the Attribute to SQL Column class for each attribute in the
        key element.

        Returns
        -------
            None
        """
        for this_attr in self.key_element.list_attributes:
            self.columns.append(AttributeToSQLColumn(this_attr, self.phi_allowed, self.sql_server_type))

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

    def _create_columns(self) -> None:
        """
        Create the columns for the given Key Element

        Returns
        -------
            None
        """
        self._create_foreign_key_columns()
        self._create_attribute_columns()

        # Instance Of columns could have an intermediary table ActInstToProcCode style
        # self._create_instance_based_columns()

    def sql_table(self) -> str:
        """
        Using the data provided by the Key Element, Attributes, and Relationships,
        generates the text to allow the creation of the SQL table

        Returns
        -------
            str
                the table creation text that contains all columns that should be stored
        """
        if len(self.columns) == 0:
            self._create_columns()

        _column_sql_text = [x.column_creation_text for x in self.columns]
        _column_sql_text.insert(0, self.identity_column)
        _column_sql_text.append(self.history_user_column)
        _column_sql_text.append(self.history_timestamp_column)
        _joined_field_list = ",\n".join(_column_sql_text)
        _text = f'{self.table_prefix} (\n{_joined_field_list}\n)\n{self.table_suffix}\n'
        return _text


class CustomTable(SQLTable):
    """
    A SQL table defined by static column definitions rather than O3 key elements.

    Serves as the base class for tables with manually specified columns,
    such as standard value lookup tables and patient identifier mapping tables.
    """

    def __init__(self, sql_server_type: SupportedSQLServers, title: str, static_columns: dict[str, str]):
        super().__init__(sql_server_type)
        self.table_name = leave_only_letters_numbers_or_underscore(title)
        self.static_columns = static_columns

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

        return _text


class StandardListTableCreator(CustomTable):
    """
    Base class to create Standard List tables
    """

    def __init__(self, sql_server_type: SupportedSQLServers, title: str, items: list[O3StandardValue]):
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

        table_name = leave_only_letters_numbers_or_underscore(title)
        dialect = get_dialect(sql_server_type)

        static_columns = {
            "key_element": f"KeyElement {dialect.string_type_short(256)} NOT NULL",
            "attribute": f"Attribute {dialect.string_type_short(256)} NOT NULL",
            "standard_value_item": f"StandardValueItemName {dialect.string_type_short(256)} NOT NULL",
            "numeric_code": f"NumericCode {dialect.string_type_short(32)} NOT NULL",
            "active_flag": f"ActiveFlag {dialect.boolean_type} NOT NULL DEFAULT 1",
            "unique_constraint": dialect.unique_constraint("AK_NumericCode", "NumericCode"),
            "index": dialect.create_index(
                "IX_StandardValueLookup_NumericCode", table_name,
                "NumericCode", ["KeyElement", "Attribute"],
            ),
        }

        super().__init__(sql_server_type, title, static_columns)

        self.items = items
        self.columns = [self.identity_column,
                        self.static_columns["key_element"],
                        self.static_columns["attribute"],
                        self.static_columns["standard_value_item"],
                        self.static_columns["numeric_code"],
                        self.static_columns["active_flag"],
                        self.history_user_column,
                        self.history_timestamp_column,
                        self.static_columns["unique_constraint"]
                        ]

    def insert_commands(self, batch_size: int = 100) -> list[str]:
        """
        Generates multi-row insert commands for the standard value list items,
        batched into groups for efficiency.

        Parameters
        ----------
        batch_size: int
            the number of rows per INSERT statement (default 100)

        Returns
        -------
            list[str]
                the commands used to insert values into the table
        """
        _commands = [self.static_columns["index"]]

        _rows = []
        for x in self.items:
            _ke_code = leave_only_letters_numbers_or_underscore(x.key_element.string_code)
            _attr_code = leave_only_letters_numbers_or_underscore(x.attribute.string_code)
            _value_name = leave_letters_numbers_spaces_underscores_dashes(x.value_name)
            _numeric_code = leave_only_letters_numbers_or_underscore(str(x.numeric_code))
            _rows.append(f"('{_ke_code}', '{_attr_code}', '{_value_name}', "
                         f"'{_numeric_code}', 'db_creation')")

        _insert_prefix = (f"INSERT INTO {self.table_name} (KeyElement, Attribute, "
                          f"StandardValueItemName, NumericCode, HistoryUser) VALUES\n")

        for i in range(0, len(_rows), batch_size):
            _batch = _rows[i:i + batch_size]
            _commands.append(_insert_prefix + ",\n".join(_batch) + ";\n")

        _commands.append("\n")
        return _commands


class LookupTableCreator(StandardListTableCreator):
    """
    An inherited class that generates a single lookup table for all standard value lists.
    """

    def __init__(self, sql_server_type: SupportedSQLServers, items: list[O3StandardValue]):
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


class PatientIdentifierHash(CustomTable):
    """
    Table for mapping patient identifiers to anonymized values.

    The MRNHash column stores a hashed representation of the MRN.
    The MRN must be hashed at the application layer before insertion
    into this table — plaintext MRNs must never be stored.
    """

    def __init__(self, sql_server_type: SupportedSQLServers, table_name: str):
        """
        Instantiates a new Patient Identifier Hash Table Creator class used for generating
        a table for the mapping MRN hashes to different anonymized values

        Parameters
        ----------
        sql_server_type: SupportedSQLServers
            the SQL server type to use
        table_name: str
            the table name
        """

        dialect = get_dialect(sql_server_type)

        static_columns = {
            "PatientId": f"PatientId {dialect.integer_type} NOT NULL",
            "MRNHash": f"MRNHash {dialect.string_type} NOT NULL",
            "AnonPatID": f"AnonPatID {dialect.string_type} NOT NULL",
            "SetName": f"SetName {dialect.string_type} NOT NULL",
        }

        super().__init__(sql_server_type, table_name, static_columns)

        self.columns = [self.identity_column,
                        self.static_columns["PatientId"],
                        self.static_columns["MRNHash"],
                        self.static_columns["AnonPatID"],
                        self.static_columns["SetName"],
                        self.history_user_column,
                        self.history_timestamp_column,
                        ]
        self.foreign_key = (f'ALTER TABLE {self.table_name} '
                            f'ADD CONSTRAINT fk_{self.table_name}_Patient '
                            f'FOREIGN KEY (PatientId) REFERENCES Patient (PatientId) '
                            f'ON DELETE RESTRICT ON UPDATE CASCADE;')


if __name__ == "__main__":
    pass
