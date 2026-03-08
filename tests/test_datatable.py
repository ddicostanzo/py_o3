"""Tests for Datatable class and query path resolution with mocked pyodbc."""

import logging
import os
import sys
from unittest.mock import MagicMock

import pytest

# Mock pyodbc before importing Datatable, with a real Error class.
# Only create the mock if pyodbc is not already in sys.modules — this avoids
# re-creating the Error class when multiple test files run in the same process.
if 'pyodbc' not in sys.modules:
    _mock_pyodbc = MagicMock()
    _mock_pyodbc.Error = type('Error', (Exception,), {})
    sys.modules['pyodbc'] = _mock_pyodbc

from sql.aria_integration.queried_datatable import Datatable, _resolve_query_path
import sql.aria_integration.queried_datatable as datatable_module


class TestDatatableInit:
    """Tests for Datatable initialization."""

    def test_query_location_stored(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()
        dt = Datatable(mock_conn, str(query_file))
        assert dt.query_location == str(query_file)

    def test_query_read_from_file(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()
        dt = Datatable(mock_conn, str(query_file))
        assert dt.query == "SELECT 1"

    def test_connection_stored(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()
        dt = Datatable(mock_conn, str(query_file))
        assert dt.connection is mock_conn

    def test_multiline_query(self, tmp_path):
        query_file = tmp_path / "multi.sql"
        query_file.write_text("SELECT col1,\n       col2\nFROM table1\nWHERE id = 1;")
        mock_conn = MagicMock()
        dt = Datatable(mock_conn, str(query_file))
        assert "SELECT col1" in dt.query
        assert "WHERE id = 1" in dt.query


class TestGetDataReturnsGenerator:
    """Test that _get_data with num_results=None returns a generator."""

    def test_returns_generator_when_num_results_none(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()
        dt = Datatable(mock_conn, str(query_file))

        result = dt._get_data(num_results=None)
        # Should be a generator (from _data_generator)
        import types
        assert isinstance(result, types.GeneratorType)


class TestGetDataReturnsList:
    """Test that _get_data with num_results returns a list."""

    def test_returns_list_when_num_results_specified(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()

        # Set up cursor mock to return rows
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchmany.return_value = [("row1",), ("row2",)]
        mock_conn.cursor.return_value = mock_cursor

        dt = Datatable(mock_conn, str(query_file))
        result = dt._get_data(num_results=5)
        assert isinstance(result, list)


class TestDataGeneratorErrorWrapping:
    """Test that _data_generator wraps pyodbc errors with query context."""

    def test_data_generator_wraps_error_with_query_path(self, tmp_path):
        pyodbc = sys.modules['pyodbc']
        query_file = tmp_path / "bad.sql"
        query_file.write_text("SELECT bad_query")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pyodbc.Error("Test DB error")
        mock_conn.cursor.return_value = mock_cursor

        dt = Datatable(mock_conn, str(query_file))
        gen = dt._data_generator()
        with pytest.raises(RuntimeError, match="bad.sql"):
            next(gen)


class TestDataRowsErrorWrapping:
    """Test that _data_rows wraps pyodbc errors with query context."""

    def test_data_rows_wraps_error_with_query_path(self, tmp_path):
        pyodbc = sys.modules['pyodbc']
        query_file = tmp_path / "bad.sql"
        query_file.write_text("SELECT bad_query")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pyodbc.Error("Test DB error")
        mock_conn.cursor.return_value = mock_cursor

        dt = Datatable(mock_conn, str(query_file))
        with pytest.raises(RuntimeError, match="bad.sql"):
            dt._data_rows(10)


class TestGetDataLogging:
    """Test that _get_data logs the query location."""

    def test_logs_query_location(self, tmp_path, caplog):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()
        dt = Datatable(mock_conn, str(query_file))

        with caplog.at_level(logging.INFO):
            # Call _get_data; the generator won't be consumed but logging should happen
            dt._get_data(num_results=None)

        assert any(str(query_file) in record.message for record in caplog.records)

    def test_log_message_contains_executing_query(self, tmp_path, caplog):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()
        dt = Datatable(mock_conn, str(query_file))

        with caplog.at_level(logging.INFO):
            dt._get_data(num_results=None)

        assert any("Executing query" in record.message for record in caplog.records)


class TestParameterizedExecution:
    """Test that params are forwarded to cursor.execute."""

    def test_generator_forwards_params_to_execute(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT * FROM t WHERE id = ?")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = iter([("row1",)])
        mock_conn.cursor.return_value = mock_cursor

        dt = Datatable(mock_conn, str(query_file))
        gen = dt._data_generator(params=("abc",))
        list(gen)

        mock_cursor.execute.assert_called_once_with(dt.query, ("abc",))

    def test_generator_without_params_calls_execute_without_params(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT 1")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = iter([])
        mock_conn.cursor.return_value = mock_cursor

        dt = Datatable(mock_conn, str(query_file))
        gen = dt._data_generator(params=None)
        list(gen)

        mock_cursor.execute.assert_called_once_with(dt.query)

    def test_data_rows_forwards_params_to_execute(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT * FROM t WHERE id = ?")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchmany.return_value = [("row1",)]
        mock_conn.cursor.return_value = mock_cursor

        dt = Datatable(mock_conn, str(query_file))
        dt._data_rows(10, params=("xyz",))

        mock_cursor.execute.assert_called_once_with(dt.query, ("xyz",))

    def test_get_data_passes_params_through(self, tmp_path):
        query_file = tmp_path / "test.sql"
        query_file.write_text("SELECT * FROM t WHERE id = ?")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchmany.return_value = [("row1",)]
        mock_conn.cursor.return_value = mock_cursor

        dt = Datatable(mock_conn, str(query_file))
        dt._get_data(num_results=5, params=("val",))

        mock_cursor.execute.assert_called_once_with(dt.query, ("val",))


class TestResolveQueryPath:
    """Test the _resolve_query_path helper function."""

    def test_resolves_existing_file(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "Aura").mkdir()
        query_file = queries_dir / "Aura" / "test.sql"
        query_file.write_text("SELECT 1")

        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)
        result = _resolve_query_path("Aura/test.sql")
        assert result == str(query_file)

    def test_returns_absolute_path(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        query_file = queries_dir / "test.sql"
        query_file.write_text("SELECT 1")

        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)
        result = _resolve_query_path("test.sql")
        assert os.path.isabs(result)

    def test_raises_file_not_found_for_missing_file(self, tmp_path, monkeypatch):
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()

        monkeypatch.setattr(datatable_module, "_QUERIES_DIR", queries_dir)
        with pytest.raises(FileNotFoundError, match="Query file not found"):
            _resolve_query_path("nonexistent.sql")
