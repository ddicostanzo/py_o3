from unittest.mock import MagicMock, PropertyMock

import pytest

from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.table_generator import (
    CustomTable,
    KeyElementTableCreator,
    PatientIdentifierHash,
    SQLTable,
)


class TestSQLTableBase:
    """Tests for the base SQLTable class properties."""

    def test_mssql_table_prefix(self):
        table = SQLTable(SupportedSQLServers.MSSQL)
        table.table_name = "Patient"
        assert table.table_prefix == "CREATE TABLE Patient"

    def test_psql_table_prefix(self):
        table = SQLTable(SupportedSQLServers.PSQL)
        table.table_name = "Patient"
        assert table.table_prefix == "CREATE TABLE Patient"

    def test_mssql_table_suffix_has_system_versioning(self):
        table = SQLTable(SupportedSQLServers.MSSQL)
        table.table_name = "Patient"
        suffix = table.table_suffix
        assert "SYSTEM_VERSIONING" in suffix
        assert "PatientHistory" in suffix

    def test_psql_table_suffix_is_semicolon(self):
        table = SQLTable(SupportedSQLServers.PSQL)
        table.table_name = "Patient"
        assert table.table_suffix == ";"

    def test_mssql_identity_column_uses_int_identity(self):
        table = SQLTable(SupportedSQLServers.MSSQL)
        table.table_name = "Patient"
        assert "PatientId" in table.identity_column
        assert "INT IDENTITY(1, 1)" in table.identity_column
        assert "PRIMARY KEY" in table.identity_column

    def test_psql_identity_column_uses_serial(self):
        table = SQLTable(SupportedSQLServers.PSQL)
        table.table_name = "Patient"
        assert "PatientId" in table.identity_column
        assert "SERIAL" in table.identity_column
        assert "PRIMARY KEY" in table.identity_column

    def test_mssql_history_timestamp_has_valid_from_to(self):
        table = SQLTable(SupportedSQLServers.MSSQL)
        col = table.history_timestamp_column
        assert "ValidFrom" in col
        assert "ValidTo" in col
        assert "PERIOD FOR SYSTEM_TIME" in col

    def test_psql_history_timestamp_uses_timestamptz(self):
        table = SQLTable(SupportedSQLServers.PSQL)
        col = table.history_timestamp_column
        assert "HistoryDateTime" in col
        assert "timestamptz" in col

    def test_mssql_history_user_uses_varchar(self):
        table = SQLTable(SupportedSQLServers.MSSQL)
        assert "varchar(max)" in table.history_user_column

    def test_psql_history_user_uses_text(self):
        table = SQLTable(SupportedSQLServers.PSQL)
        assert "text" in table.history_user_column

    def test_invalid_server_type_raises(self):
        with pytest.raises(ValueError):
            SQLTable("INVALID")

    def test_unknown_dialect_name_raises(self):
        mock_dialect = MagicMock()
        type(mock_dialect).name = PropertyMock(return_value="MySQL")
        type(mock_dialect).type_map = PropertyMock(return_value={})
        type(mock_dialect).string_type = PropertyMock(return_value="text")
        type(mock_dialect).integer_type = PropertyMock(return_value="int")
        type(mock_dialect).boolean_type = PropertyMock(return_value="boolean")
        with pytest.raises(ValueError, match="Unknown dialect name"):
            SQLTable(mock_dialect)


class TestCustomTable:
    """Tests for CustomTable: table name sanitization and SQL output."""

    def test_table_name_sanitized(self):
        table = CustomTable(
            SupportedSQLServers.PSQL,
            "My Table!",
            {"col1": "col1 text NOT NULL"},
        )
        assert table.table_name == "MyTable"

    def test_sql_table_output_format(self):
        table = CustomTable(
            SupportedSQLServers.PSQL,
            "TestTable",
            {"col1": "col1 text NOT NULL"},
        )
        table.columns = ["TestTableId SERIAL PRIMARY KEY", "col1 text NOT NULL"]
        sql = table.sql_table()
        assert "CREATE TABLE TestTable" in sql
        assert "TestTableId SERIAL PRIMARY KEY" in sql
        assert "col1 text NOT NULL" in sql
        assert sql.strip().endswith(";")

    def test_special_chars_removed_from_table_name(self):
        table = CustomTable(
            SupportedSQLServers.MSSQL,
            "Patient-Info @2024",
            {},
        )
        assert table.table_name == "PatientInfo2024"


class TestKeyElementTableCreator:
    """Tests for KeyElementTableCreator: table name from string_code,
    SQL output structure."""

    def _mock_key_element(self, string_code="KEL_Patient", attributes=None, relationships=None):
        ke = MagicMock()
        ke.string_code = string_code
        ke.list_attributes = attributes or []
        ke.child_of_relationships = relationships or []
        ke.instance_of_relationships = []
        return ke

    def test_table_name_from_string_code(self):
        ke = self._mock_key_element(string_code="KEL_Patient")
        creator = KeyElementTableCreator(SupportedSQLServers.PSQL, ke)
        assert creator.table_name == "KEL_Patient"

    def test_table_name_sanitized_from_string_code(self):
        ke = self._mock_key_element(string_code="KEL Patient-Info!")
        creator = KeyElementTableCreator(SupportedSQLServers.PSQL, ke)
        assert creator.table_name == "KELPatientInfo"

    def test_sql_table_includes_identity_column(self):
        ke = self._mock_key_element(string_code="Patient")
        creator = KeyElementTableCreator(SupportedSQLServers.PSQL, ke, phi_allowed=False)
        sql = creator.sql_table()
        assert "PatientId SERIAL PRIMARY KEY" in sql

    def test_sql_table_includes_history_columns(self):
        ke = self._mock_key_element(string_code="Patient")
        creator = KeyElementTableCreator(SupportedSQLServers.PSQL, ke, phi_allowed=False)
        sql = creator.sql_table()
        assert "HistoryUser" in sql
        assert "HistoryDateTime" in sql

    def test_sql_table_creates_table_statement(self):
        ke = self._mock_key_element(string_code="Patient")
        creator = KeyElementTableCreator(SupportedSQLServers.PSQL, ke, phi_allowed=False)
        sql = creator.sql_table()
        assert sql.startswith("CREATE TABLE Patient")

    def test_mssql_sql_table_has_system_versioning(self):
        ke = self._mock_key_element(string_code="Patient")
        creator = KeyElementTableCreator(SupportedSQLServers.MSSQL, ke, phi_allowed=False)
        sql = creator.sql_table()
        assert "SYSTEM_VERSIONING" in sql
        assert "PatientHistory" in sql

    def test_mssql_identity_uses_int_identity(self):
        ke = self._mock_key_element(string_code="Patient")
        creator = KeyElementTableCreator(SupportedSQLServers.MSSQL, ke, phi_allowed=False)
        sql = creator.sql_table()
        assert "PatientId INT IDENTITY(1, 1) NOT NULL PRIMARY KEY" in sql


class TestPatientIdentifierHash:
    """Tests for PatientIdentifierHash table structure."""

    def test_mssql_columns_use_varchar(self):
        table = PatientIdentifierHash(SupportedSQLServers.MSSQL, "PatientHash")
        sql = table.sql_table()
        assert "varchar(max)" in sql

    def test_psql_columns_use_text(self):
        table = PatientIdentifierHash(SupportedSQLServers.PSQL, "PatientHash")
        sql = table.sql_table()
        assert "text" in sql

    def test_table_name_sanitized(self):
        table = PatientIdentifierHash(SupportedSQLServers.PSQL, "Patient Hash!")
        assert table.table_name == "PatientHash"

    def test_foreign_key_constraint_generated(self):
        table = PatientIdentifierHash(SupportedSQLServers.MSSQL, "PatientHash")
        assert "FOREIGN KEY" in table.foreign_key
        assert "PatientId" in table.foreign_key
        assert "Patient" in table.foreign_key

    def test_column_named_mrnhash_not_mrn(self):
        table = PatientIdentifierHash(SupportedSQLServers.MSSQL, "PatientHash")
        sql = table.sql_table()
        assert "MRNHash" in sql
        assert "MRN " not in sql
        assert "MRN\t" not in sql

    def test_psql_column_named_mrnhash_not_mrn(self):
        table = PatientIdentifierHash(SupportedSQLServers.PSQL, "PatientHash")
        sql = table.sql_table()
        assert "MRNHash" in sql
        assert "MRN " not in sql

    def test_mssql_foreign_key_uses_no_action_on_delete(self):
        table = PatientIdentifierHash(SupportedSQLServers.MSSQL, "PatientHash")
        assert "ON DELETE NO ACTION" in table.foreign_key
        assert "ON DELETE CASCADE" not in table.foreign_key

    def test_psql_foreign_key_uses_restrict_on_delete(self):
        table = PatientIdentifierHash(SupportedSQLServers.PSQL, "PatientHash")
        assert "ON DELETE RESTRICT" in table.foreign_key

    def test_foreign_key_uses_cascade_on_update(self):
        table = PatientIdentifierHash(SupportedSQLServers.MSSQL, "PatientHash")
        assert "ON UPDATE CASCADE" in table.foreign_key

    def test_static_columns_key_is_mrnhash(self):
        table = PatientIdentifierHash(SupportedSQLServers.MSSQL, "PatientHash")
        assert "MRNHash" in table.static_columns
        assert "MRN" not in table.static_columns
