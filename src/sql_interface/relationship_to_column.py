from helpers.string_helpers import leave_only_letters_numbers_or_underscore
from src.helpers.test_sql_server_type import check_sql_server_type
from src.helpers.enums import SupportedSQLServers

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.base.o3_relationship import O3Relationship
    from src.helpers.enums import SupportedSQLServers


class ChildRelationshipToColumn:
    """
    The Child Relationship column adds the primary key from the predicate element to the subject element's table.
    """
    def __init__(self, relationship: O3Relationship, sql_server_type: SupportedSQLServers):
        """
        Instantiates a child relationship column using the relationship and SQL server type.

        Parameters
        ----------
        relationship: O3Relationship
            the relationship to create the column from
        sql_server_type:SupportedSQLServers
            the SQL server type
        """
        super().__init__()
        if not check_sql_server_type(sql_server_type):
            raise Exception("Unsupported SQL Server Type")

        self.relationship = relationship
        self.sql_server_type = sql_server_type

    @property
    def __column_name(self) -> str:
        """
        The column name after being parsed to remove spaces and special characters.

        Returns
        -------
            str
                the column name
        """
        return f"{leave_only_letters_numbers_or_underscore(self.relationship.predicate_element)}Id"

    @property
    def __column_type(self) -> str:
        """
        The column type depending on the SQL version being used.

        Returns
        -------
            str
                either INT or INTEGER depending on the SQL version being used.
        """
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return "INT"
        else:
            return "INTEGER"

    @property
    def column_creation_text(self) -> str:
        """
        The full SQL command to add this column.

        Returns
        -------
            str
                the full SQL command
        """
        return f"{self.__column_name} {self.__column_type} NOT NULL"


class InstanceRelationshipToColumn:
    def __init__(self, relationship, sql_server_type):
        super().__init__()
        if not check_sql_server_type(sql_server_type):
            raise Exception("Unsupported SQL Server Type")

        self.relationship = relationship
        self.sql_server_type = sql_server_type

    @property
    def __column_name(self):
        return f"{leave_only_letters_numbers_or_underscore(self.relationship.subject_element)}Id"

    @property
    def __column_type(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return "INT"
        else:
            return "INTEGER"

    @property
    def column_creation_text(self):
        return f"{self.__column_name} {self.__column_type} NULL"


if __name__ == "__main__":
    pass
