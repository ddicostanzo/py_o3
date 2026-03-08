# tests/etl/test_loader.py
from unittest.mock import MagicMock
import pytest
from etl.pipeline.loader import LoadCommand, Loader
from etl.pipeline.extractor import ExtractQuery
from etl.mapping.mapping_store import CrosswalkEntry
from etl.registry import JoinSpec


def _make_entry(**overrides) -> CrosswalkEntry:
    base = {
        "dwh_table": "DWH.FactActivityBilling",
        "dwh_column": "DimPatientID",
        "model_name": "dwActivityBillingModel",
        "model_alias": "PatientId",
        "model_expr": "PatientId",
        "o3_key_element": "Patient",
        "o3_attribute": "PatientIdentifier",
        "confidence": 0.92,
        "status": "confirmed",
    }
    base.update(overrides)
    return CrosswalkEntry(**base)


def _make_extract_query(entries=None) -> ExtractQuery:
    return ExtractQuery(
        entry_point="billing",
        model_name="dwActivityBillingModel",
        base_table="DWH.FactActivityBilling",
        sql="SELECT ...",
        date_key="DimDateID_FromDateOfService",
        joins=[],
        columns_mapped=entries or [_make_entry()],
    )


def _mock_o3_model():
    model = MagicMock()
    ke = MagicMock()
    ke.string_code = "KEL_Patient"
    ke.key_element_name = "Patient"
    model.key_elements = {"Patient": ke}
    return model


class TestGenerateInsert:
    def test_returns_load_command(self):
        loader = Loader(_mock_o3_model())
        cmd = loader.generate_insert(_make_extract_query())
        assert isinstance(cmd, LoadCommand)
        assert "INSERT" in cmd.sql
        assert "Patient" in cmd.target_table

    def test_column_map_populated(self):
        loader = Loader(_mock_o3_model())
        cmd = loader.generate_insert(_make_extract_query())
        assert "PatientIdentifier" in cmd.column_map

    def test_empty_column_map_raises(self):
        inactive_entry = _make_entry(status="rejected")
        loader = Loader(_mock_o3_model())
        with pytest.raises(ValueError, match="No O3 key elements"):
            loader.generate_insert(_make_extract_query(entries=[inactive_entry]))


class TestGenerateMerge:
    def test_returns_merge_command(self):
        loader = Loader(_mock_o3_model())
        cmd = loader.generate_merge(_make_extract_query(), merge_key=["PatientIdentifier"])
        assert isinstance(cmd, LoadCommand)
        assert "MERGE" in cmd.sql

    def test_merge_key_in_on_clause(self):
        loader = Loader(_mock_o3_model())
        cmd = loader.generate_merge(_make_extract_query(), merge_key=["PatientIdentifier"])
        assert "PatientIdentifier" in cmd.sql

    def test_invalid_merge_key_raises(self):
        loader = Loader(_mock_o3_model())
        with pytest.raises(ValueError, match="Merge keys"):
            loader.generate_merge(_make_extract_query(), merge_key=["NonexistentKey"])
