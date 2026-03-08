from unittest.mock import MagicMock

from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.foreign_keys import ForeignKeysConstraints


def _mock_relationship(subject="Patient", predicate="Diagnosis", category="ChildElement-Of", cardinality="1:N"):
    rel = MagicMock()
    rel.subject_element = subject
    rel.predicate_element = predicate
    rel.relationship_category = category
    rel.cardinality = cardinality
    return rel


class TestForeignKeysConstraintsDefaults:
    """Tests for ForeignKeysConstraints default behavior."""

    def test_default_on_delete_is_restrict(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.MSSQL)
        assert "ON DELETE RESTRICT" in fk.column_creation_text

    def test_default_on_update_is_cascade(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.MSSQL)
        assert "ON UPDATE CASCADE" in fk.column_creation_text

    def test_default_does_not_use_delete_cascade(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.MSSQL)
        assert "ON DELETE CASCADE" not in fk.column_creation_text


class TestForeignKeysConstraintsCustomOnDelete:
    """Tests for ForeignKeysConstraints with custom on_delete parameter."""

    def test_custom_on_delete_cascade(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.MSSQL, on_delete="CASCADE")
        assert "ON DELETE CASCADE" in fk.column_creation_text

    def test_custom_on_delete_no_action(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.PSQL, on_delete="NO ACTION")
        assert "ON DELETE NO ACTION" in fk.column_creation_text

    def test_custom_on_delete_set_null(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.PSQL, on_delete="SET NULL")
        assert "ON DELETE SET NULL" in fk.column_creation_text

    def test_on_update_always_cascade_regardless_of_on_delete(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.MSSQL, on_delete="SET NULL")
        assert "ON UPDATE CASCADE" in fk.column_creation_text


class TestForeignKeysConstraintsOutput:
    """Tests for the full SQL output of ForeignKeysConstraints."""

    def test_column_creation_text_structure(self):
        rel = _mock_relationship(subject="Treatment", predicate="Patient")
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.MSSQL)
        text = fk.column_creation_text
        assert "ALTER TABLE Treatment" in text
        assert "ADD CONSTRAINT fk_Treatment_Patient" in text
        assert "FOREIGN KEY (PatientId)" in text
        assert "REFERENCES Patient (PatientId)" in text
        assert text.strip().endswith(";")

    def test_psql_default_restrict(self):
        rel = _mock_relationship()
        fk = ForeignKeysConstraints(rel, SupportedSQLServers.PSQL)
        assert "ON DELETE RESTRICT" in fk.column_creation_text
