"""Tests for PatientInformation class with mocked pyodbc."""

import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock pyodbc before importing PatientInformation.
# Only create the mock if pyodbc is not already in sys.modules.
if 'pyodbc' not in sys.modules:
    _mock_pyodbc = MagicMock()
    _mock_pyodbc.Error = type('Error', (Exception,), {})
    sys.modules['pyodbc'] = _mock_pyodbc

from sql.aria_integration.patient_information import PatientInformation


class TestPatientInformationInit:
    """Tests for PatientInformation initialization."""

    def test_query_file_constant(self):
        assert PatientInformation._QUERY_FILE == 'Aura/patient_information.sql'

    @patch('sql.aria_integration.patient_information._resolve_query_path')
    def test_uses_resolve_query_path(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient_information.sql"
        query_file.write_text("SELECT 1 WHERE id = ?")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        pi = PatientInformation(mock_conn)

        mock_resolve.assert_called_once_with('Aura/patient_information.sql')
        assert "?" in pi.query


class TestPatientInformationGetData:
    """Tests for PatientInformation.get_data method."""

    @patch('sql.aria_integration.patient_information._resolve_query_path')
    def test_passes_mrn_as_params(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient_information.sql"
        query_file.write_text("SELECT * FROM t WHERE id = ?")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = iter([("row1",)])
        mock_conn.cursor.return_value = mock_cursor

        pi = PatientInformation(mock_conn)
        gen = pi.get_data('12345')
        list(gen)

        mock_cursor.execute.assert_called_once_with(pi.query, ('12345',))

    @patch('sql.aria_integration.patient_information._resolve_query_path')
    def test_empty_mrn_raises_value_error(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient_information.sql"
        query_file.write_text("SELECT 1 WHERE id = ?")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        pi = PatientInformation(mock_conn)

        with pytest.raises(ValueError, match="mrn must be a non-empty string"):
            pi.get_data('')

    @patch('sql.aria_integration.patient_information._resolve_query_path')
    def test_none_mrn_raises_value_error(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient_information.sql"
        query_file.write_text("SELECT 1 WHERE id = ?")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        pi = PatientInformation(mock_conn)

        with pytest.raises(ValueError, match="mrn must be a non-empty string"):
            pi.get_data(None)


class TestPatientInformationGetDataWithNumResults:
    """Tests for PatientInformation.get_data with num_results."""

    @patch('sql.aria_integration.patient_information._resolve_query_path')
    def test_forwards_num_results(self, mock_resolve, tmp_path):
        query_file = tmp_path / "patient_information.sql"
        query_file.write_text("SELECT * FROM t WHERE id = ?")
        mock_resolve.return_value = str(query_file)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchmany.return_value = [("row1",)]
        mock_conn.cursor.return_value = mock_cursor

        pi = PatientInformation(mock_conn)
        result = pi.get_data('12345', num_results=10)

        assert isinstance(result, list)
        mock_cursor.execute.assert_called_once_with(pi.query, ('12345',))
        mock_cursor.execute.return_value.fetchmany.assert_called_once_with(10)
