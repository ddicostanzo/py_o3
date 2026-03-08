"""Tests for Patient class with mocked pyodbc."""

import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock pyodbc before importing Patient.
# Only create the mock if pyodbc is not already in sys.modules.
if 'pyodbc' not in sys.modules:
    _mock_pyodbc = MagicMock()
    _mock_pyodbc.Error = type('Error', (Exception,), {})
    sys.modules['pyodbc'] = _mock_pyodbc

from sql.aria_integration.patient import Patient


class TestPatientInit:
    """Tests for Patient initialization."""

    def test_query_file_constant(self):
        assert Patient._QUERY_FILE == 'Aura/patient.sql'

    @patch('sql.aria_integration.patient._resolve_query_path')
    def test_uses_resolve_query_path(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient.sql"
        query_file.write_text("SELECT 1")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        patient = Patient(mock_conn)

        mock_resolve.assert_called_once_with('Aura/patient.sql')
        assert patient.query == "SELECT 1"


class TestPatientGetData:
    """Tests for Patient.get_data method."""

    @patch('sql.aria_integration.patient._resolve_query_path')
    def test_get_data_defaults_to_generator(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient.sql"
        query_file.write_text("SELECT 1")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        patient = Patient(mock_conn)

        import types
        result = patient.get_data()
        assert isinstance(result, types.GeneratorType)

    @patch('sql.aria_integration.patient._resolve_query_path')
    def test_get_data_forwards_num_results(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient.sql"
        query_file.write_text("SELECT 1")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchmany.return_value = [("row1",)]
        mock_conn.cursor.return_value = mock_cursor

        patient = Patient(mock_conn)
        result = patient.get_data(num_results=5)

        assert isinstance(result, list)
        mock_cursor.execute.return_value.fetchmany.assert_called_once_with(5)
