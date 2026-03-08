"""Tests for PatientInformation class with mocked pyodbc."""

import sys
from unittest.mock import MagicMock

import pytest

# Mock pyodbc before importing PatientInformation.
# Only create the mock if pyodbc is not already in sys.modules.
if 'pyodbc' not in sys.modules:
    _mock_pyodbc = MagicMock()
    _mock_pyodbc.Error = type('Error', (Exception,), {})
    sys.modules['pyodbc'] = _mock_pyodbc

import sql.aria_integration.queried_datatable as datatable_module
from sql.aria_integration.patient_information import PatientInformation


class TestPatientInformationInit:
    """Tests for PatientInformation initialization."""

    def test_query_file_constant(self):
        assert PatientInformation._QUERY_FILE == 'Aura/patient_information.sql'

    def test_resolves_query_path(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        query_file = queries_dir / "Aura" / "patient_information.sql"
        query_file.write_text("SELECT 1 WHERE id = ?")

        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)
        mock_conn = MagicMock()
        pi = PatientInformation(mock_conn)

        assert pi.query_location == str(query_file)
        assert "?" in pi.query


class TestPatientInformationGetData:
    """Tests for PatientInformation.get_data method."""

    def test_passes_mrn_as_params(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        (queries_dir / "Aura" / "patient_information.sql").write_text(
            "SELECT * FROM t WHERE id = ?"
        )
        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = iter([("row1",)])
        mock_conn.cursor.return_value = mock_cursor

        pi = PatientInformation(mock_conn)
        gen = pi.get_data('12345')
        list(gen)

        mock_cursor.execute.assert_called_once_with(pi.query, ('12345',))

    def test_empty_mrn_raises_value_error(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        (queries_dir / "Aura" / "patient_information.sql").write_text(
            "SELECT 1 WHERE id = ?"
        )
        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)

        mock_conn = MagicMock()
        pi = PatientInformation(mock_conn)

        with pytest.raises(ValueError, match="mrn must be a non-empty string"):
            pi.get_data('')

    def test_none_mrn_raises_value_error(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        (queries_dir / "Aura" / "patient_information.sql").write_text(
            "SELECT 1 WHERE id = ?"
        )
        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)

        mock_conn = MagicMock()
        pi = PatientInformation(mock_conn)

        with pytest.raises(ValueError, match="mrn must be a non-empty string"):
            pi.get_data(None)


class TestPatientInformationGetDataWithNumResults:
    """Tests for PatientInformation.get_data with num_results."""

    def test_forwards_num_results(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        (queries_dir / "Aura" / "patient_information.sql").write_text(
            "SELECT * FROM t WHERE id = ?"
        )
        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchmany.return_value = [("row1",)]
        mock_conn.cursor.return_value = mock_cursor

        pi = PatientInformation(mock_conn)
        result = pi.get_data('12345', num_results=10)

        assert isinstance(result, list)
        mock_cursor.execute.assert_called_once_with(pi.query, ('12345',))
        mock_cursor.execute.return_value.fetchmany.assert_called_once_with(10)
