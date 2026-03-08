import sys
import warnings
from unittest.mock import MagicMock

import pytest

# Mock pyodbc before importing MSSQLConnection, since pyodbc requires
# native ODBC drivers that may not be available in all environments.
sys.modules.setdefault('pyodbc', MagicMock())

from sql.connection.mssql import MSSQLConnection


def _make_config(**overrides) -> dict[str, str]:
    """Create a valid config dictionary with optional overrides."""
    base = {
        'DRIVER': '{ODBC Driver 17 for SQL Server}',
        'SERVER': 'localhost',
        'DATABASE': 'TestDB',
        'SCHEMA': 'dbo',
        'AUTH': 'SQL',
        'USERID': 'sa',
        'PASSWORD': 'secret',
    }
    base.update(overrides)
    return base


class TestMissingKeyRaisesValueError:
    """Tests that missing required keys raise ValueError with a helpful message."""

    @pytest.mark.parametrize("missing_key", [
        'DRIVER', 'SERVER', 'DATABASE', 'SCHEMA', 'AUTH', 'USERID', 'PASSWORD',
    ])
    def test_missing_required_key_raises_value_error(self, missing_key):
        config = _make_config()
        del config[missing_key]
        with pytest.raises(ValueError, match=f"Missing required config key '{missing_key}'"):
            MSSQLConnection(config)

    def test_error_message_lists_all_required_keys(self):
        config = _make_config()
        del config['DRIVER']
        with pytest.raises(ValueError, match="Required keys are:.*DRIVER.*SERVER.*DATABASE"):
            MSSQLConnection(config)

    def test_error_message_mentions_prefix_format(self):
        config = _make_config()
        del config['DRIVER']
        with pytest.raises(ValueError, match=r"O3_\* or AURA_\*"):
            MSSQLConnection(config)


class TestTLSWarningEncrypt:
    """Tests for TLS warnings when Encrypt is not 'yes'."""

    def test_warns_when_encrypt_is_no(self):
        config = _make_config(ENCRYPT='no')
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            MSSQLConnection(config)
            encrypt_warnings = [x for x in w if "Encrypt" in str(x.message)]
            assert len(encrypt_warnings) == 1

    def test_warns_when_encrypt_is_optional(self):
        config = _make_config(ENCRYPT='optional')
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            MSSQLConnection(config)
            encrypt_warnings = [x for x in w if "Encrypt" in str(x.message)]
            assert len(encrypt_warnings) == 1


class TestTLSWarningTrustServerCertificate:
    """Tests for TLS warnings when TrustServerCertificate is 'yes'."""

    def test_warns_when_trust_server_certificate_is_yes(self):
        config = _make_config(TRUSTSERVERCERTIFICATE='yes')
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            MSSQLConnection(config)
            trust_warnings = [x for x in w if "TrustServerCertificate" in str(x.message)]
            assert len(trust_warnings) == 1


class TestNoWarningWithSecureDefaults:
    """Tests that no warnings are raised when secure defaults are used."""

    def test_no_warnings_with_explicit_secure_values(self):
        config = _make_config(ENCRYPT='yes', TRUSTSERVERCERTIFICATE='no')
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            MSSQLConnection(config)
            tls_warnings = [
                x for x in w
                if "Encrypt" in str(x.message) or "TrustServerCertificate" in str(x.message)
            ]
            assert len(tls_warnings) == 0

    def test_no_warnings_with_defaults(self):
        config = _make_config()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            MSSQLConnection(config)
            tls_warnings = [
                x for x in w
                if "Encrypt" in str(x.message) or "TrustServerCertificate" in str(x.message)
            ]
            assert len(tls_warnings) == 0


class TestDefaultEncrypt:
    """Tests that Encrypt defaults to 'yes' when not in config."""

    def test_default_encrypt_is_yes(self):
        config = _make_config()
        # No ENCRYPT key in config
        conn = MSSQLConnection(config)
        assert conn.encrypt == 'yes'

    def test_explicit_encrypt_overrides_default(self):
        config = _make_config(ENCRYPT='no')
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            conn = MSSQLConnection(config)
        assert conn.encrypt == 'no'


class TestDefaultTrustServerCertificate:
    """Tests that TrustServerCertificate defaults to 'no' when not in config."""

    def test_default_trust_server_certificate_is_no(self):
        config = _make_config()
        # No TRUSTSERVERCERTIFICATE key in config
        conn = MSSQLConnection(config)
        assert conn.trust_server_cert == 'no'

    def test_explicit_trust_server_certificate_overrides_default(self):
        config = _make_config(TRUSTSERVERCERTIFICATE='yes')
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            conn = MSSQLConnection(config)
        assert conn.trust_server_cert == 'yes'


class TestConnectionStringAssembly:
    """Tests for connection string assembly for SQL and Integrated auth."""

    def test_sql_auth_connection_includes_uid_pwd(self):
        config = _make_config(AUTH='SQL')
        conn = MSSQLConnection(config)
        # Access the private connection_string method via name mangling
        conn_dict = conn._MSSQLConnection__connection_string()
        assert conn_dict['Trusted_Connection'] == 'no'
        assert conn_dict['UID'] == 'sa'
        assert conn_dict['PWD'] == 'secret'

    def test_integrated_auth_connection_uses_trusted(self):
        config = _make_config(AUTH='INTEGRATED')
        conn = MSSQLConnection(config)
        conn_dict = conn._MSSQLConnection__connection_string()
        assert conn_dict['Trusted_Connection'] == 'yes'
        assert 'UID' not in conn_dict
        assert 'PWD' not in conn_dict

    def test_sql_auth_includes_driver_server_database(self):
        config = _make_config()
        conn = MSSQLConnection(config)
        conn_dict = conn._MSSQLConnection__connection_string()
        assert conn_dict['Driver'] == '{ODBC Driver 17 for SQL Server}'
        assert conn_dict['Server'] == 'localhost'
        assert conn_dict['Database'] == 'TestDB'

    def test_sql_auth_includes_encrypt_and_trust(self):
        config = _make_config()
        conn = MSSQLConnection(config)
        conn_dict = conn._MSSQLConnection__connection_string()
        assert conn_dict['Encrypt'] == 'yes'
        assert conn_dict['TrustServerCertificate'] == 'no'

    def test_invalid_auth_raises_value_error(self):
        config = _make_config(AUTH='SQL')
        conn = MSSQLConnection(config)
        # Manually set to invalid auth to trigger the else branch
        conn.authentication = 'INVALID'
        with pytest.raises(ValueError, match="Provided server not supported"):
            conn._MSSQLConnection__connection_string()
