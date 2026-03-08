"""Tests for Patient class with mocked pyodbc."""

import sys
from unittest.mock import MagicMock

import pytest

# Mock pyodbc before importing Patient.
# Only create the mock if pyodbc is not already in sys.modules.
if 'pyodbc' not in sys.modules:
    _mock_pyodbc = MagicMock()
    _mock_pyodbc.Error = type('Error', (Exception,), {})
    sys.modules['pyodbc'] = _mock_pyodbc

import sql.aria_integration.queried_datatable as datatable_module
from sql.aria_integration.patient import Patient


class TestPatientInit:
    """Tests for Patient initialization."""

    def test_query_file_constant(self):
        assert Patient._QUERY_FILE == 'Aura/patient.sql'

    def test_resolves_query_path(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        query_file = queries_dir / "Aura" / "patient.sql"
        query_file.write_text("SELECT 1")

        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)
        mock_conn = MagicMock()
        patient = Patient(mock_conn)

        assert patient.query_location == str(query_file)
        assert patient.query == "SELECT 1"


class TestPatientGetData:
    """Tests for Patient.get_data method."""

    def test_get_data_defaults_to_generator(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        (queries_dir / "Aura" / "patient.sql").write_text("SELECT 1")
        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)

        mock_conn = MagicMock()
        patient = Patient(mock_conn)

        import types
        result = patient.get_data()
        assert isinstance(result, types.GeneratorType)

    def test_get_data_forwards_num_results(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        (queries_dir / "Aura" / "patient.sql").write_text("SELECT 1")
        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchmany.return_value = [("row1",)]
        mock_conn.cursor.return_value = mock_cursor

        patient = Patient(mock_conn)
        result = patient.get_data(num_results=5)

        assert isinstance(result, list)
        mock_cursor.execute.return_value.fetchmany.assert_called_once_with(5)

    def test_get_data_does_not_pass_params(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        (queries_dir / "Aura").mkdir(parents=True)
        (queries_dir / "Aura" / "patient.sql").write_text("SELECT 1")
        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = iter([("row1",)])
        mock_conn.cursor.return_value = mock_cursor

        patient = Patient(mock_conn)
        gen = patient.get_data()
        list(gen)

        mock_cursor.execute.assert_called_once_with(patient.query)
