# tests/etl/test_runner.py
import os
import tempfile
from unittest.mock import MagicMock, patch, PropertyMock
import pytest
from etl.pipeline.runner import ETLRunner, ETLResult, EntryPointResult
from etl.pipeline.extractor import ExtractQuery, Extractor
from etl.pipeline.loader import LoadCommand, Loader
from etl.mapping.mapping_store import CrosswalkEntry


def _make_extract_query() -> ExtractQuery:
    return ExtractQuery(
        entry_point="billing",
        model_name="dwActivityBillingModel",
        base_table="DWH.FactActivityBilling",
        sql="SELECT TOP 1000 base.[DimPatientID] FROM DWH.FactActivityBilling AS base",
        date_key="DimDateID_FromDateOfService",
        joins=[],
        columns_mapped=[],
    )


def _make_load_command() -> LoadCommand:
    return LoadCommand(
        target_table="KEL_Patient",
        sql="INSERT INTO KEL_Patient ([PatientIdentifier]) SELECT src.[PatientId] FROM (...) AS src",
        column_map={"PatientIdentifier": "PatientId"},
    )


def _mock_extractor() -> MagicMock:
    ext = MagicMock(spec=Extractor)
    ext.generate_query.return_value = _make_extract_query()
    ext.generate_all_queries.return_value = [_make_extract_query()]
    return ext


def _mock_loader() -> MagicMock:
    ldr = MagicMock(spec=Loader)
    ldr.generate_insert.return_value = _make_load_command()
    return ldr


class TestExportSql:
    def test_writes_sql_files(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with tempfile.TemporaryDirectory() as tmpdir:
            runner.export_sql(tmpdir)
            files = os.listdir(tmpdir)
            assert len(files) > 0
            assert any(f.endswith(".sql") for f in files)

    def test_sql_file_contains_extract_and_load(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with tempfile.TemporaryDirectory() as tmpdir:
            runner.export_sql(tmpdir)
            sql_files = [f for f in os.listdir(tmpdir) if f.endswith(".sql")]
            with open(os.path.join(tmpdir, sql_files[0])) as f:
                content = f.read()
            assert "SELECT" in content
            assert "INSERT" in content

    def test_filter_by_entry_points(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with tempfile.TemporaryDirectory() as tmpdir:
            runner.export_sql(tmpdir, entry_points=["billing"])
            files = os.listdir(tmpdir)
            assert len(files) == 1


class TestRunDryRun:
    def test_dry_run_returns_result_without_executing(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        result = runner.run(entry_points=["billing"], dry_run=True)
        assert isinstance(result, ETLResult)
        assert result.success is True
        assert len(result.results) == 1
        assert result.results[0].entry_point == "billing"
        assert result.results[0].rows_extracted == 0
        assert result.results[0].rows_loaded == 0


class TestRunLive:
    def test_live_requires_connection(self):
        runner = ETLRunner(_mock_extractor(), _mock_loader())
        with pytest.raises(ValueError, match="connection"):
            runner.run(entry_points=["billing"], dry_run=False)

    def test_extract_error_captured_with_context(self):
        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = RuntimeError("connection lost")
        runner = ETLRunner(_mock_extractor(), _mock_loader(), connection=conn)
        result = runner.run(entry_points=["billing"])
        assert result.success is False
        assert len(result.results[0].errors) == 1
        assert "Extract failed" in result.results[0].errors[0]
        assert "RuntimeError" in result.results[0].errors[0]

    def test_load_error_triggers_rollback(self):
        cursor = MagicMock()
        cursor.execute.side_effect = [None, RuntimeError("insert failed")]
        cursor.fetchall.return_value = [("row1",)]
        conn = MagicMock()
        conn.cursor.return_value = cursor
        runner = ETLRunner(_mock_extractor(), _mock_loader(), connection=conn)
        result = runner.run(entry_points=["billing"])
        assert result.success is False
        assert "Load failed" in result.results[0].errors[0]
        conn.rollback.assert_called_once()
