"""Mapping of O3 relationships to SQL foreign key column definitions."""
from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.enums import SupportedSQLServers
from helpers.string_helpers import leave_only_letters_numbers_or_underscore
from helpers.validate_sql_server_type import check_sql_server_type
from sql.dialects import get_dialect

if TYPE_CHECKING:
    from base.o3_relationship import O3Relationship


class RelationshipToColumn:
    """
    Base class for relationship-based SQL column generation.

    Maps an O3 relationship to a foreign key column definition,
    using a configurable element field for the column name and
    nullable constraint.
    """

    def __init__(self, relationship: O3Relationship, sql_server_type: SupportedSQLServers,
                 element_field: str, nullable: str):
        """
        Instantiates a relationship column using the relationship and SQL server type.

        Parameters
        ----------
        relationship : O3Relationship
            the relationship to create the column from
        sql_server_type : SupportedSQLServers
            the SQL server type
        element_field : str
            the attribute name on the relationship to use for the column name
            (e.g., "predicate_element" or "subject_element")
        nullable : str
            the nullable constraint (e.g., "NOT NULL" or "NULL")
        """
        super().__init__()
        if not check_sql_server_type(sql_server_type):
            raise ValueError("Unsupported SQL Server Type")

        self.relationship = relationship
        self.sql_server_type = sql_server_type
        self.dialect = get_dialect(sql_server_type)
        self._element_field = element_field
        self._nullable = nullable

    @property
    def _column_name(self) -> str:
        """
        The column name after being parsed to remove spaces and special characters.

        Returns
        -------
            str
                the column name
        """
        element_value = getattr(self.relationship, self._element_field)
        return f"{leave_only_letters_numbers_or_underscore(element_value)}Id"

    @property
    def _column_type(self) -> str:
        """
        The column type depending on the SQL version being used.

        Returns
        -------
            str
                either INT or INTEGER depending on the SQL version being used.
        """
        return self.dialect.integer_type.upper()

    @property
    def column_creation_text(self) -> str:
        """
        The full SQL command to add this column.

        Returns
        -------
            str
                the full SQL command
        """
        return f"{self._column_name} {self._column_type} {self._nullable}"


class ChildRelationshipToColumn(RelationshipToColumn):
    """
    The Child Relationship column adds the primary key from the predicate element to the subject element's table.
    """

    def __init__(self, relationship: O3Relationship, sql_server_type: SupportedSQLServers):
        """
        Instantiates a child relationship column using the relationship and SQL server type.

        Parameters
        ----------
        relationship : O3Relationship
            the relationship to create the column from
        sql_server_type : SupportedSQLServers
            the SQL server type
        """
        super().__init__(relationship, sql_server_type,
                         element_field="predicate_element", nullable="NOT NULL")


class InstanceRelationshipToColumn(RelationshipToColumn):
    """
    The Instance Relationship column adds the primary key from the subject element.
    """

    def __init__(self, relationship: O3Relationship, sql_server_type: SupportedSQLServers):
        """
        Instantiates an instance relationship column using the relationship and SQL server type.

        Parameters
        ----------
        relationship : O3Relationship
            the relationship to create the column from
        sql_server_type : SupportedSQLServers
            the SQL server type
        """
        super().__init__(relationship, sql_server_type,
                         element_field="subject_element", nullable="NULL")


if __name__ == "__main__":
    pass
