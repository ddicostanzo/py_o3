from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.sql_type_from_o3_data_type import sql_data_types


class TestSQLTypeFromO3DataTypeMSSQL:
    server = SupportedSQLServers.MSSQL

    def test_boolean(self):
        assert sql_data_types[self.server]["Boolean"] == "bit"

    def test_binary(self):
        assert sql_data_types[self.server]["Binary"] == "varbinary"

    def test_date(self):
        assert sql_data_types[self.server]["Date"] == "datetime2"

    def test_decimal(self):
        assert sql_data_types[self.server]["Decimal"] == "decimal(19,9)"

    def test_integer(self):
        assert sql_data_types[self.server]["Integer"] == "int"

    def test_string(self):
        assert sql_data_types[self.server]["String"] == "varchar(max)"


class TestSQLTypeFromO3DataTypePSQL:
    server = SupportedSQLServers.PSQL

    def test_boolean(self):
        assert sql_data_types[self.server]["Boolean"] == "boolean"

    def test_binary(self):
        assert sql_data_types[self.server]["Binary"] == "bytea"

    def test_date(self):
        assert sql_data_types[self.server]["Date"] == "timestamptz"

    def test_decimal(self):
        assert sql_data_types[self.server]["Decimal"] == "numeric(19,9)"

    def test_integer(self):
        assert sql_data_types[self.server]["Integer"] == "integer"

    def test_string(self):
        assert sql_data_types[self.server]["String"] == "text"
