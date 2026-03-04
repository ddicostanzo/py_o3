import pytest

from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.add_columns import add_column_sql_command, add_foreign_key_column_sql_command


class TestAddColumnSQLCommand:
    def test_mssql_generates_valid_statement(self):
        result = add_column_sql_command(
            "Patient", "Age", "int", nullable=True,
            sql_server_type=SupportedSQLServers.MSSQL,
        )
        assert result == "ALTER TABLE Patient ADD COLUMN Age int NULL;"

    def test_psql_generates_valid_statement(self):
        result = add_column_sql_command(
            "Patient", "Age", "integer", nullable=False,
            sql_server_type=SupportedSQLServers.PSQL,
        )
        assert result == "ALTER TABLE Patient ADD COLUMN Age integer NOT NULL;"

    def test_nullable_true_produces_null(self):
        result = add_column_sql_command(
            "Tbl", "Col", "text", nullable=True,
            sql_server_type=SupportedSQLServers.PSQL,
        )
        assert "NULL" in result
        assert "NOT NULL" not in result

    def test_nullable_false_produces_not_null(self):
        result = add_column_sql_command(
            "Tbl", "Col", "text", nullable=False,
            sql_server_type=SupportedSQLServers.PSQL,
        )
        assert "NOT NULL" in result

    def test_special_chars_sanitized_from_table_and_column(self):
        result = add_column_sql_command(
            "My Table!", "Col@Name", "text", nullable=True,
            sql_server_type=SupportedSQLServers.PSQL,
        )
        assert "My Table!" not in result
        assert "Col@Name" not in result
        assert "MyTable" in result
        assert "ColName" in result

    def test_mssql_uses_int_for_integer(self):
        """MSSQL maps Integer to 'int' (lowercase)."""
        result = add_column_sql_command(
            "Patient", "Age", "int", nullable=True,
            sql_server_type=SupportedSQLServers.MSSQL,
        )
        assert " int " in result

    def test_psql_uses_integer(self):
        """PSQL maps Integer to 'integer' (lowercase)."""
        result = add_column_sql_command(
            "Patient", "Age", "integer", nullable=True,
            sql_server_type=SupportedSQLServers.PSQL,
        )
        assert " integer " in result

    def test_invalid_column_type_raises_type_error(self):
        with pytest.raises(TypeError):
            add_column_sql_command(
                "Tbl", "Col", "INVALID_TYPE", nullable=True,
                sql_server_type=SupportedSQLServers.PSQL,
            )


class TestAddForeignKeyColumnSQLCommand:
    def test_mssql_case_mismatch_raises_type_error(self):
        """Known bug: add_foreign_key_column_sql_command passes 'INT' (uppercase)
        but sql_data_types has 'int' (lowercase), causing a TypeError.
        This test documents the current behavior before the fix is applied."""
        with pytest.raises(TypeError, match="Column type INT does not exist"):
            add_foreign_key_column_sql_command(
                "Patient", "DoctorId",
                sql_server_type=SupportedSQLServers.MSSQL,
            )

    def test_psql_case_mismatch_raises_type_error(self):
        """Known bug: add_foreign_key_column_sql_command passes 'INTEGER' (uppercase)
        but sql_data_types has 'integer' (lowercase), causing a TypeError.
        This test documents the current behavior before the fix is applied."""
        with pytest.raises(TypeError, match="Column type INTEGER does not exist"):
            add_foreign_key_column_sql_command(
                "Patient", "DoctorId",
                sql_server_type=SupportedSQLServers.PSQL,
            )
