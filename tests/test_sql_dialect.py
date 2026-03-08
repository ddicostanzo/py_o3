import pytest

from helpers.enums import SupportedSQLServers
from sql.dialect import SQLDialect
from sql.dialects import get_dialect
from sql.dialects.mssql_dialect import MSSQLDialect
from sql.dialects.psql_dialect import PSQLDialect


class TestMSSQLDialect:
    """Tests for MSSQLDialect implementation."""

    def setup_method(self):
        self.dialect = MSSQLDialect()

    def test_name(self):
        assert self.dialect.name == "MSSQL"

    def test_string_type(self):
        assert self.dialect.string_type == "varchar(max)"

    def test_integer_type(self):
        assert self.dialect.integer_type == "int"

    def test_boolean_type(self):
        assert self.dialect.boolean_type == "bit"

    def test_type_map_boolean(self):
        assert self.dialect.type_map["Boolean"] == "bit"

    def test_type_map_binary(self):
        assert self.dialect.type_map["Binary"] == "varbinary"

    def test_type_map_date(self):
        assert self.dialect.type_map["Date"] == "datetime2"

    def test_type_map_decimal(self):
        assert self.dialect.type_map["Decimal"] == "decimal(19,9)"

    def test_type_map_integer(self):
        assert self.dialect.type_map["Integer"] == "int"

    def test_type_map_string(self):
        assert self.dialect.type_map["String"] == "varchar(max)"

    def test_identity_column(self):
        result = self.dialect.identity_column("Patient")
        assert result == "PatientId INT IDENTITY(1, 1) NOT NULL PRIMARY KEY"

    def test_history_timestamp_columns(self):
        result = self.dialect.history_timestamp_columns()
        assert "ValidFrom" in result
        assert "ValidTo" in result
        assert "PERIOD FOR SYSTEM_TIME" in result

    def test_history_user_column(self):
        assert self.dialect.history_user_column() == "HistoryUser varchar(max) NOT NULL"

    def test_table_suffix(self):
        result = self.dialect.table_suffix("Patient")
        assert "SYSTEM_VERSIONING" in result
        assert "PatientHistory" in result

    def test_alter_table_add_column(self):
        result = self.dialect.alter_table_add_column("Patient", "Age", "int", "NULL")
        assert result == "ALTER TABLE Patient ADD Age int NULL;"

    def test_alter_table_add_column_not_null(self):
        result = self.dialect.alter_table_add_column("Patient", "Name", "varchar(max)", "NOT NULL")
        assert result == "ALTER TABLE Patient ADD Name varchar(max) NOT NULL;"

    def test_string_type_short_default(self):
        assert self.dialect.string_type_short() == "varchar(256)"

    def test_string_type_short_custom(self):
        assert self.dialect.string_type_short(128) == "varchar(128)"

    def test_unique_constraint(self):
        result = self.dialect.unique_constraint("AK_Code", "CodeColumn")
        assert result == "CONSTRAINT AK_Code Unique(CodeColumn)"

    def test_create_index(self):
        result = self.dialect.create_index("IX_Test", "MyTable", "Col1", ["Col2", "Col3"])
        assert "CREATE NONCLUSTERED INDEX IX_Test ON MyTable" in result
        assert "(Col1)" in result
        assert "INCLUDE (Col2, Col3)" in result

    def test_on_delete_restrict_returns_no_action(self):
        assert self.dialect.on_delete_restrict == "NO ACTION"

    def test_boolean_default_true(self):
        assert self.dialect.boolean_default_true == "DEFAULT 1"


class TestPSQLDialect:
    """Tests for PSQLDialect implementation."""

    def setup_method(self):
        self.dialect = PSQLDialect()

    def test_name(self):
        assert self.dialect.name == "PSQL"

    def test_string_type(self):
        assert self.dialect.string_type == "text"

    def test_integer_type(self):
        assert self.dialect.integer_type == "integer"

    def test_boolean_type(self):
        assert self.dialect.boolean_type == "boolean"

    def test_type_map_boolean(self):
        assert self.dialect.type_map["Boolean"] == "boolean"

    def test_type_map_binary(self):
        assert self.dialect.type_map["Binary"] == "bytea"

    def test_type_map_date(self):
        assert self.dialect.type_map["Date"] == "timestamptz"

    def test_type_map_decimal(self):
        assert self.dialect.type_map["Decimal"] == "numeric(19,9)"

    def test_type_map_integer(self):
        assert self.dialect.type_map["Integer"] == "integer"

    def test_type_map_string(self):
        assert self.dialect.type_map["String"] == "text"

    def test_identity_column(self):
        result = self.dialect.identity_column("Patient")
        assert result == "PatientId SERIAL PRIMARY KEY"

    def test_history_timestamp_columns(self):
        result = self.dialect.history_timestamp_columns()
        assert "HistoryDateTime" in result
        assert "timestamptz" in result

    def test_history_user_column(self):
        assert self.dialect.history_user_column() == "HistoryUser text NOT NULL"

    def test_table_suffix(self):
        assert self.dialect.table_suffix("Patient") == ";"

    def test_alter_table_add_column(self):
        result = self.dialect.alter_table_add_column("Patient", "Age", "integer", "NULL")
        assert result == "ALTER TABLE Patient ADD COLUMN Age integer NULL;"

    def test_alter_table_add_column_not_null(self):
        result = self.dialect.alter_table_add_column("Patient", "Name", "text", "NOT NULL")
        assert result == "ALTER TABLE Patient ADD COLUMN Name text NOT NULL;"

    def test_string_type_short_returns_text(self):
        assert self.dialect.string_type_short() == "text"

    def test_string_type_short_ignores_max_length(self):
        assert self.dialect.string_type_short(128) == "text"

    def test_unique_constraint(self):
        result = self.dialect.unique_constraint("AK_Code", "CodeColumn")
        assert result == "Unique(CodeColumn)"

    def test_create_index(self):
        result = self.dialect.create_index("IX_Test", "MyTable", "Col1", ["Col2", "Col3"])
        assert "CREATE INDEX IX_Test ON MyTable" in result
        assert "(Col1)" in result
        assert "INCLUDE (Col2, Col3)" in result

    def test_on_delete_restrict_returns_restrict(self):
        assert self.dialect.on_delete_restrict == "RESTRICT"

    def test_boolean_default_true(self):
        assert self.dialect.boolean_default_true == "DEFAULT TRUE"


class TestGetDialectFactory:
    """Tests for the get_dialect factory function."""

    def test_returns_mssql_dialect_for_mssql(self):
        dialect = get_dialect(SupportedSQLServers.MSSQL)
        assert isinstance(dialect, MSSQLDialect)

    def test_returns_psql_dialect_for_psql(self):
        dialect = get_dialect(SupportedSQLServers.PSQL)
        assert isinstance(dialect, PSQLDialect)

    def test_raises_for_invalid_server_type(self):
        with pytest.raises((ValueError, TypeError)):
            get_dialect("INVALID")

    def test_mssql_dialect_name_matches(self):
        dialect = get_dialect(SupportedSQLServers.MSSQL)
        assert dialect.name == "MSSQL"

    def test_psql_dialect_name_matches(self):
        dialect = get_dialect(SupportedSQLServers.PSQL)
        assert dialect.name == "PSQL"


class TestProtocolCompliance:
    """Tests that dialect implementations satisfy the SQLDialect Protocol."""

    def test_mssql_is_sql_dialect(self):
        assert isinstance(MSSQLDialect(), SQLDialect)

    def test_psql_is_sql_dialect(self):
        assert isinstance(PSQLDialect(), SQLDialect)

    def test_factory_returns_sql_dialect(self):
        mssql = get_dialect(SupportedSQLServers.MSSQL)
        psql = get_dialect(SupportedSQLServers.PSQL)
        assert isinstance(mssql, SQLDialect)
        assert isinstance(psql, SQLDialect)


