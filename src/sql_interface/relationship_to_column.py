from helpers.string_helpers import clean_table_and_column_names
from src.helpers.test_sql_server_type import check_sql_server_type
from src.helpers.enums import SupportedSQLServers


class ChildRelationshipToColumn:
    def __init__(self, relationship, sql_server_type):
        super().__init__()
        if not check_sql_server_type(sql_server_type):
            raise Exception("Unsupported SQL Server Type")

        self.relationship = relationship
        self.sql_server_type = sql_server_type

    @property
    def __column_name(self):
        return f"{clean_table_and_column_names(self.relationship.predicate_element)}Id"

    @property
    def __column_type(self):
        if self.sql_server_type == SupportedSQLServers.MSSQL:
            return "INT"
        else:
            return "INTEGER"

    @property
    def column_creation_text(self):
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
        return f"{clean_table_and_column_names(self.relationship.subject_element)}Id"

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
