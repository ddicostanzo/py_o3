from src.base.o3_relationship import O3Relationship
from src.helpers.test_sql_server_type import check_sql_server_type


class ForeignKeysConstraints:
    def __init__(self, relationship: O3Relationship, sql_server_type):
        if not check_sql_server_type(sql_server_type):
            raise Exception("Unsupported SQL Server Type")

        self._relationship = relationship
        self.sql_server_type = sql_server_type
        self.subject_element = relationship.subject_element
        self.subject_table_name = relationship.subject_element.replace(' ', '')
        self.predicate_element = relationship.predicate_element
        self.predicate_table_name = self.predicate_element.replace(' ', '')
        self.relationship_category = relationship.relationship_category
        self.cardinality = relationship.cardinality
        self.fk_name = f'fk_{self.subject_table_name}_{self.predicate_table_name}'

    def __command_prefix(self):
        return f"ALTER TABLE {self.subject_table_name} ADD CONSTRAINT {self.fk_name}"

    def __command_body(self):
        return (f"FOREIGN KEY ({self.predicate_element}Id) "
                f"REFERENCES {self.predicate_table_name} ({self.predicate_element}Id)")

    def __command_suffix(self):
        return f"ON DELETE CASCADE ON UPDATE CASCADE"

    @property
    def column_creation_text(self):
        return f"{self.__command_prefix()} {self.__command_body()} {self.__command_suffix()};\n"


if __name__ == '__main__':
    pass
