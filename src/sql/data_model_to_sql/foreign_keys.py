"""Foreign key constraint generation from O3 relationships."""
from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.string_helpers import leave_only_letters_numbers_or_underscore
from helpers.validate_sql_server_type import check_sql_server_type

if TYPE_CHECKING:
    from base.o3_relationship import O3Relationship
    from helpers.enums import SupportedSQLServers


class ForeignKeysConstraints:
    """
    The class to instantiate foreign keys constraints.
    """

    def __init__(self, relationship: O3Relationship, sql_server_type: SupportedSQLServers,
                 on_delete: str = "RESTRICT"):
        """
        Instantiate foreign keys constraints from an O3 relationship.

        Parameters
        ----------
        relationship: O3Relationship
            the relationship to create the foriegn keys constraints
        sql_server_type: SupportedSQLServers
            the SQL server type to generate the commands
        on_delete: str
            the ON DELETE action to use (default: "RESTRICT")
        """
        if not check_sql_server_type(sql_server_type):
            raise ValueError("Unsupported SQL Server Type")

        self._relationship = relationship
        self.sql_server_type = sql_server_type
        self._on_delete = on_delete
        self.subject_element = relationship.subject_element
        self.subject_table_name = leave_only_letters_numbers_or_underscore(relationship.subject_element)
        self.predicate_element = relationship.predicate_element
        self.predicate_table_name = leave_only_letters_numbers_or_underscore(self.predicate_element)
        self.relationship_category = relationship.relationship_category
        self.cardinality = relationship.cardinality
        self.fk_name = f'fk_{self.subject_table_name}_{self.predicate_table_name}'

    def __command_prefix(self) -> str:
        """
        The prefix of the command including the alter table with appropriate table name and adding the constraint

        Returns
        -------
            str
                the command prefix
        """
        return f"ALTER TABLE {self.subject_table_name} ADD CONSTRAINT {self.fk_name}"

    def __command_body(self) -> str:
        """
        The body of the command including the predicate element table name and primary key to be included in this
        table.

        Returns
        -------
            str
                the command body
        """
        return (f"FOREIGN KEY ({self.predicate_table_name}Id) "
                f"REFERENCES {self.predicate_table_name} ({self.predicate_table_name}Id)")

    def __command_suffix(self) -> str:
        """
        The SQL command suffix including the update and delete action commands.

        Returns
        -------
            str
                the SQL command suffix
        """
        return f"ON DELETE {self._on_delete} ON UPDATE CASCADE"

    @property
    def column_creation_text(self) -> str:
        """
        The full SQL command that has compiled the prefix, body, and suffix into a single statement.

        Returns
        -------
            str
                the SQL command
        """
        return f"{self.__command_prefix()} {self.__command_body()} {self.__command_suffix()};\n"


if __name__ == '__main__':
    pass
