"""Tests for the workflow orchestration functions in api/workflow.py."""
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from api.workflow import (
    create_individual_standard_value_tables,
    create_key_element_tables,
    create_model,
    create_standard_value_lookup_table,
    create_tables,
    foreign_key_constraints,
    get_table_names_from_relationships,
    validate_names_in_relationships,
    write_sql_to_text,
)
from helpers.enums import SupportedSQLServers


def _mock_relationship(subject="Patient", category="ChildElement-Of", predicate="Diagnosis"):
    rel = MagicMock()
    rel.subject_element = subject
    rel.relationship_category = category
    rel.predicate_element = predicate
    return rel


def _mock_key_element(name="Patient", string_code="Patient", attributes=None,
                      relationships=None, child_of_relationships=None):
    ke = MagicMock()
    ke.key_element_name = name
    ke.string_code = string_code
    ke.list_attributes = attributes or []
    ke.relationships = relationships or []
    ke.child_of_relationships = child_of_relationships or []
    return ke


def _mock_model(key_elements=None, standard_value_lists=None):
    model = MagicMock()
    model.key_elements = key_elements or {}
    model.standard_value_lists = standard_value_lists or {}
    return model


class TestCreateKeyElementTables:
    def test_returns_dict_with_table_names(self):
        ke = _mock_key_element(name="Patient", string_code="Patient")
        model = _mock_model(key_elements={"Patient": ke})

        with patch("api.workflow.KeyElementTableCreator") as mock_creator:
            mock_creator.return_value.sql_table.return_value = "CREATE TABLE Patient (...)"
            result = create_key_element_tables(model, SupportedSQLServers.MSSQL, phi_allowed=True)

        assert "Patient" in result
        assert result["Patient"] == "CREATE TABLE Patient (...)"

    def test_creates_table_for_each_key_element(self):
        ke1 = _mock_key_element(name="Patient")
        ke2 = _mock_key_element(name="Diagnosis")
        model = _mock_model(key_elements={"Patient": ke1, "Diagnosis": ke2})

        with patch("api.workflow.KeyElementTableCreator") as mock_creator:
            mock_creator.return_value.sql_table.return_value = "SQL"
            result = create_key_element_tables(model, SupportedSQLServers.PSQL, phi_allowed=False)

        assert len(result) == 2
        assert mock_creator.call_count == 2

    def test_empty_model_returns_empty_dict(self):
        model = _mock_model(key_elements={})
        result = create_key_element_tables(model, SupportedSQLServers.MSSQL, phi_allowed=False)
        assert result == {}


class TestCreateTables:
    def test_delegates_to_create_key_element_tables(self):
        ke = _mock_key_element(name="Patient")
        model = _mock_model(key_elements={"Patient": ke})

        with patch("api.workflow.KeyElementTableCreator") as mock_creator:
            mock_creator.return_value.sql_table.return_value = "SQL"
            result = create_tables(model, SupportedSQLServers.MSSQL, phi_allowed=True)

        assert "Patient" in result


class TestCreateIndividualStandardValueTables:
    def test_creates_table_per_standard_value_list(self):
        sv1 = [MagicMock()]
        sv2 = [MagicMock()]
        model = _mock_model(standard_value_lists={"Gender": sv1, "Laterality": sv2})

        with patch("api.workflow.StandardListTableCreator") as mock_creator:
            mock_creator.return_value.sql_table.return_value = "SQL"
            result = create_individual_standard_value_tables(model, SupportedSQLServers.PSQL)

        assert len(result) == 2
        assert "Gender" in result
        assert "Laterality" in result

    def test_empty_standard_values_returns_empty(self):
        model = _mock_model(standard_value_lists={})
        result = create_individual_standard_value_tables(model, SupportedSQLServers.MSSQL)
        assert result == {}


class TestCreateStandardValueLookupTable:
    def test_returns_lookup_table_creator(self):
        sv = [MagicMock(), MagicMock()]
        model = _mock_model(standard_value_lists={"Gender": sv})

        with patch("api.workflow.LookupTableCreator") as mock_creator:
            mock_creator.return_value = MagicMock()
            result = create_standard_value_lookup_table(model, SupportedSQLServers.MSSQL)

        assert result is mock_creator.return_value

    def test_passes_all_standard_values(self):
        sv1 = [MagicMock(), MagicMock()]
        sv2 = [MagicMock()]
        model = _mock_model(standard_value_lists={"A": sv1, "B": sv2})

        with patch("api.workflow.LookupTableCreator") as mock_creator:
            create_standard_value_lookup_table(model, SupportedSQLServers.PSQL)

        args = mock_creator.call_args
        assert len(args[0][1]) == 3  # All items flattened


class TestForeignKeyConstraints:
    def test_generates_constraint_per_child_relationship(self):
        rel1 = _mock_relationship()
        rel2 = _mock_relationship(subject="Treatment", predicate="Patient")
        ke = _mock_key_element(child_of_relationships=[rel1, rel2])
        model = _mock_model(key_elements={"Patient": ke})

        with patch("api.workflow.ForeignKeysConstraints") as mock_fk:
            mock_fk.return_value.column_creation_text = "ALTER TABLE ..."
            result = foreign_key_constraints(model, SupportedSQLServers.MSSQL)

        assert len(result) == 2
        assert mock_fk.call_count == 2

    def test_no_child_relationships_returns_empty(self):
        ke = _mock_key_element(child_of_relationships=[])
        model = _mock_model(key_elements={"Patient": ke})

        result = foreign_key_constraints(model, SupportedSQLServers.PSQL)
        assert result == []


class TestGetTableNamesFromRelationships:
    def test_returns_subject_predicate_category_sets(self):
        rel1 = _mock_relationship("Patient", "ChildElement-Of", "Diagnosis")
        rel2 = _mock_relationship("Treatment", "InstanceAssociated-with", "Patient")
        ke = _mock_key_element(relationships=[rel1, rel2])
        model = _mock_model(key_elements={"Patient": ke})

        subjects, predicates, categories = get_table_names_from_relationships(model)

        assert subjects == {"Patient", "Treatment"}
        assert predicates == {"Diagnosis", "Patient"}
        assert categories == {"ChildElement-Of", "InstanceAssociated-with"}

    def test_empty_relationships_returns_empty_sets(self):
        ke = _mock_key_element(relationships=[])
        model = _mock_model(key_elements={"Patient": ke})

        subjects, predicates, categories = get_table_names_from_relationships(model)

        assert subjects == set()
        assert predicates == set()
        assert categories == set()


class TestWriteSqlToText:
    def test_writes_commands_to_file(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            path = f.name

        try:
            write_sql_to_text(path, ["CREATE TABLE t1;\n", "CREATE TABLE t2;\n"], write_mode='w')
            with open(path) as f:
                content = f.read()
            assert "CREATE TABLE t1;" in content
            assert "CREATE TABLE t2;" in content
        finally:
            os.unlink(path)

    def test_append_mode_adds_to_existing(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("-- existing\n")
            path = f.name

        try:
            write_sql_to_text(path, ["-- appended\n"], write_mode='a')
            with open(path) as f:
                content = f.read()
            assert "-- existing" in content
            assert "-- appended" in content
        finally:
            os.unlink(path)

    def test_default_mode_is_append(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("-- first\n")
            path = f.name

        try:
            write_sql_to_text(path, ["-- second\n"])
            with open(path) as f:
                content = f.read()
            assert "-- first" in content
            assert "-- second" in content
        finally:
            os.unlink(path)


class TestWriteSqlToTextValidation:
    def test_rejects_read_mode(self):
        with pytest.raises(ValueError, match="write_mode must be 'w' or 'a'"):
            write_sql_to_text("/tmp/test.sql", ["SELECT 1;"], write_mode='r')

    def test_rejects_arbitrary_mode(self):
        with pytest.raises(ValueError, match="write_mode must be 'w' or 'a'"):
            write_sql_to_text("/tmp/test.sql", ["SELECT 1;"], write_mode='rb')


_SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), '..', 'src', 'Resources', 'O3_20250128.json'
)


class TestCreateModel:
    @pytest.mark.skipif(
        not os.path.exists(_SCHEMA_PATH),
        reason="Schema file O3_20250128.json not available in Resources/"
    )
    def test_create_model_with_valid_json(self):
        model = create_model(_SCHEMA_PATH, clean=True)
        assert len(model.key_elements) > 0

    def test_create_model_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            create_model("/nonexistent/path/schema.json", clean=False)


class TestValidateNamesInRelationships:
    def test_does_not_raise(self):
        rel = _mock_relationship("Patient", "ChildElement-Of", "Diagnosis")
        ke = _mock_key_element(string_code="Patient", relationships=[rel])
        model = _mock_model(key_elements={"Patient": ke})

        # Should not raise — just logs warnings
        validate_names_in_relationships({"Patient"}, {"Diagnosis"}, model)

    def test_logs_warning_for_missing_subject(self):
        ke = _mock_key_element(string_code="Treatment", relationships=[])
        model = _mock_model(key_elements={"Treatment": ke})

        import logging
        with patch.object(logging, 'warning') as mock_warn:
            validate_names_in_relationships({"Patient"}, {"Treatment"}, model)
            mock_warn.assert_any_call("String Code Treatment not in subject table names")
