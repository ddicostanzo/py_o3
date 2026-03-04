import pytest

from helpers.enums import SupportedSQLServers
from helpers.test_sql_server_type import check_sql_server_type


class TestCheckSQLServerType:
    def test_mssql_is_valid(self):
        assert check_sql_server_type(SupportedSQLServers.MSSQL) is True

    def test_psql_is_valid(self):
        assert check_sql_server_type(SupportedSQLServers.PSQL) is True

    def test_invalid_string_raises_value_error(self):
        with pytest.raises(ValueError):
            check_sql_server_type("MySQL")

    def test_invalid_int_raises_value_error(self):
        with pytest.raises(ValueError):
            check_sql_server_type(99)

    def test_none_raises_value_error(self):
        with pytest.raises(ValueError):
            check_sql_server_type(None)
