"""Integration tests that load the real O3 JSON schema and exercise the full pipeline."""

import json
import pathlib

import pytest

from api.data_model import O3DataModel
from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.foreign_keys import ForeignKeysConstraints
from sql.data_model_to_sql.table_generator import (
    KeyElementTableCreator,
    LookupTableCreator,
    StandardListTableCreator,
)

_JSON_PATH = pathlib.Path(__file__).parent.parent / 'src' / 'Resources' / 'O3_20250128_Fixed.json'


@pytest.fixture(scope="module")
def model():
    """Load the real O3 data model with cleaning enabled."""
    return O3DataModel(str(_JSON_PATH), clean=True)


class TestModelParsing:
    """Verify the data model parses the JSON without exceptions."""

    def test_key_elements_non_empty(self, model):
        assert len(model.key_elements) > 0

    def test_key_elements_have_names(self, model):
        for name, ke in model.key_elements.items():
            assert name == ke.key_element_name

    def test_all_key_elements_have_string_codes(self, model):
        for ke in model.key_elements.values():
            assert ke.string_code is not None
            assert len(ke.string_code) > 0

    def test_patient_key_element_exists(self, model):
        assert "Patient" in model.key_elements

    def test_attributes_exist_on_key_elements(self, model):
        has_attrs = any(len(ke.list_attributes) > 0 for ke in model.key_elements.values())
        assert has_attrs

    def test_relationships_exist_on_key_elements(self, model):
        has_rels = any(len(ke.relationships) > 0 for ke in model.key_elements.values())
        assert has_rels

    def test_standard_value_lists_non_empty(self, model):
        assert len(model.standard_value_lists) > 0

    def test_value_data_types_non_empty(self, model):
        assert len(model.value_data_types) > 0

    def test_allow_nulls_non_empty(self, model):
        assert len(model.allow_nulls) > 0


class TestFromDict:
    """Test O3DataModel.from_dict() classmethod."""

    def test_from_dict_creates_same_key_elements(self):
        with open(_JSON_PATH) as f:
            text = f.read()
            text = text.replace('(\\u002B Other)', 'Other')
            text = text.replace('(\\u002BOther)', 'Other')
            data = json.loads(text)

        model = O3DataModel.from_dict(data, clean=True)
        assert len(model.key_elements) > 0
        assert "Patient" in model.key_elements

    def test_from_dict_json_file_is_none(self):
        with open(_JSON_PATH) as f:
            text = f.read()
            text = text.replace('(\\u002B Other)', 'Other')
            text = text.replace('(\\u002BOther)', 'Other')
            data = json.loads(text)

        model = O3DataModel.from_dict(data)
        assert model.json_file is None


class TestMSSQLTableGeneration:
    """Generate MSSQL tables from the model and verify structure."""

    def test_create_table_for_each_key_element(self, model):
        for ke in model.key_elements.values():
            creator = KeyElementTableCreator(SupportedSQLServers.MSSQL, ke)
            sql = creator.sql_table()
            assert "CREATE TABLE" in sql
            assert creator.table_name in sql

    def test_patient_table_has_columns(self, model):
        ke = model.key_elements["Patient"]
        creator = KeyElementTableCreator(SupportedSQLServers.MSSQL, ke, phi_allowed=True)
        sql = creator.sql_table()
        assert "CREATE TABLE" in sql
        assert len(creator.columns) > 0

    def test_phi_allowed_does_not_raise(self, model):
        ke = model.key_elements["Patient"]
        creator = KeyElementTableCreator(SupportedSQLServers.MSSQL, ke, phi_allowed=True)
        sql = creator.sql_table()
        assert isinstance(sql, str)

    def test_phi_not_allowed_does_not_raise(self, model):
        ke = model.key_elements["Patient"]
        creator = KeyElementTableCreator(SupportedSQLServers.MSSQL, ke, phi_allowed=False)
        sql = creator.sql_table()
        assert isinstance(sql, str)


class TestPSQLTableGeneration:
    """Generate PSQL tables from the model and verify structure."""

    def test_create_table_for_each_key_element(self, model):
        for ke in model.key_elements.values():
            creator = KeyElementTableCreator(SupportedSQLServers.PSQL, ke)
            sql = creator.sql_table()
            assert "CREATE TABLE" in sql
            assert creator.table_name in sql

    def test_patient_table_has_columns(self, model):
        ke = model.key_elements["Patient"]
        creator = KeyElementTableCreator(SupportedSQLServers.PSQL, ke, phi_allowed=False)
        sql = creator.sql_table()
        assert "CREATE TABLE" in sql
        assert len(creator.columns) > 0


class TestForeignKeysIntegration:
    """Generate FK constraints from real relationships."""

    def test_fk_constraints_for_child_relationships(self, model):
        generated = 0
        for ke in model.key_elements.values():
            for rel in ke.child_of_relationships:
                fk = ForeignKeysConstraints(rel, SupportedSQLServers.MSSQL)
                text = fk.column_creation_text
                assert "ALTER TABLE" in text
                assert "FOREIGN KEY" in text
                assert "REFERENCES" in text
                generated += 1
        # There should be at least some child-of relationships in the model
        assert generated > 0

    def test_fk_constraints_psql(self, model):
        for ke in model.key_elements.values():
            for rel in ke.child_of_relationships:
                fk = ForeignKeysConstraints(rel, SupportedSQLServers.PSQL)
                text = fk.column_creation_text
                assert "ALTER TABLE" in text
                assert "FOREIGN KEY" in text


class TestStandardListIntegration:
    """Generate standard list and lookup tables from real data."""

    def test_standard_list_table_creation(self, model):
        for name, items in model.standard_value_lists.items():
            creator = StandardListTableCreator(SupportedSQLServers.MSSQL, name, items)
            sql = creator.sql_table()
            assert "CREATE TABLE" in sql

    def test_standard_list_insert_commands(self, model):
        for name, items in model.standard_value_lists.items():
            creator = StandardListTableCreator(SupportedSQLServers.MSSQL, name, items)
            inserts = creator.insert_commands()
            assert isinstance(inserts, list)
            assert len(inserts) > 0
            break  # one is enough

    def test_lookup_table_creation(self, model):
        all_items = []
        for items in model.standard_value_lists.values():
            all_items.extend(items)
        creator = LookupTableCreator(SupportedSQLServers.MSSQL, all_items)
        sql = creator.sql_table()
        assert "CREATE TABLE StandardValuesLookup" in sql

    def test_lookup_table_psql(self, model):
        all_items = []
        for items in model.standard_value_lists.values():
            all_items.extend(items)
        creator = LookupTableCreator(SupportedSQLServers.PSQL, all_items)
        sql = creator.sql_table()
        assert "CREATE TABLE StandardValuesLookup" in sql

    def test_lookup_insert_commands(self, model):
        all_items = []
        for items in model.standard_value_lists.values():
            all_items.extend(items)
        creator = LookupTableCreator(SupportedSQLServers.MSSQL, all_items)
        inserts = creator.insert_commands()
        assert len(inserts) > 1  # index + at least one INSERT batch + trailing newline
