from sql.dialects.mssql_dialect import MSSQLDialect
from sql.dialects.psql_dialect import PSQLDialect


class TestSQLTypeFromO3DataTypeMSSQL:
    def setup_method(self):
        self.type_map = MSSQLDialect().type_map

    def test_boolean(self):
        assert self.type_map["Boolean"] == "bit"

    def test_binary(self):
        assert self.type_map["Binary"] == "varbinary"

    def test_date(self):
        assert self.type_map["Date"] == "datetime2"

    def test_decimal(self):
        assert self.type_map["Decimal"] == "decimal(19,9)"

    def test_integer(self):
        assert self.type_map["Integer"] == "int"

    def test_string(self):
        assert self.type_map["String"] == "varchar(max)"


class TestSQLTypeFromO3DataTypePSQL:
    def setup_method(self):
        self.type_map = PSQLDialect().type_map

    def test_boolean(self):
        assert self.type_map["Boolean"] == "boolean"

    def test_binary(self):
        assert self.type_map["Binary"] == "bytea"

    def test_date(self):
        assert self.type_map["Date"] == "timestamptz"

    def test_decimal(self):
        assert self.type_map["Decimal"] == "numeric(19,9)"

    def test_integer(self):
        assert self.type_map["Integer"] == "integer"

    def test_string(self):
        assert self.type_map["String"] == "text"
