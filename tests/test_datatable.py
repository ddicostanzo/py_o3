"""Tests for Datatable class with mocked pyodbc."""

import logging
import sys
from unittest.mock import MagicMock

import pytest

# Mock pyodbc before importing Datatable, with a real Error class
_mock_pyodbc = sys.modules.get('pyodbc')
if _mock_pyodbc is None or isinstance(_mock_pyodbc, MagicMock):
    _mock_pyodbc = MagicMock()
    _mock_pyodbc.Error = type('Error', (Exception,), {})
    sys.modules['pyodbc'] = _mock_pyodbc

from sql.aria_integration.queried_datatable import Datatable


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
        # Make closing work with context manager
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

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
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

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
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

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
