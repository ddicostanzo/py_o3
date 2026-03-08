# tests/etl/test_lineage_builder.py
from unittest.mock import MagicMock
import pytest
from etl.lineage.lineage_builder import LineageBuilder, LineageGraph, LineageNode
from etl.mapping.mapping_store import CrosswalkEntry


def _make_entry(**overrides) -> CrosswalkEntry:
    base = {
        "dwh_table": "DWH.DimPatient",
        "dwh_column": "PatientId",
        "model_name": None,
        "model_alias": None,
        "model_expr": None,
        "o3_key_element": "Patient",
        "o3_attribute": "PatientIdentifier",
        "confidence": 0.92,
        "status": "confirmed",
    }
    base.update(overrides)
    return CrosswalkEntry(**base)


def _mock_o3_model():
    model = MagicMock()
    ke = MagicMock()
    ke.key_element_name = "Patient"
    attr = MagicMock()
    attr.value_name = "PatientIdentifier"
    attr.value_data_type = "String"
    attr2 = MagicMock()
    attr2.value_name = "PatientDateOfBirth"
    attr2.value_data_type = "Date"
    ke.list_attributes = [attr, attr2]
    model.key_elements = {"Patient": ke}
    return model


def _mock_manifest():
    manifest = MagicMock()
    col = MagicMock()
    col.name = "PatientId"
    col.data_type = "varchar"
    col2 = MagicMock()
    col2.name = "DateOfBirth"
    col2.data_type = "datetime"
    table = MagicMock()
    table.full_name = "DWH.DimPatient"
    table.columns = [col, col2]
    manifest.tables = {"DWH.DimPatient": table}
    manifest.models = []
    return manifest


class TestLineageBuilder:
    def test_build_direct_mapping(self):
        entries = [_make_entry()]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        assert isinstance(graph, LineageGraph)
        assert len(graph.nodes) >= 2  # source + target
        assert len(graph.edges) >= 1

    def test_build_with_transform(self):
        entries = [
            _make_entry(
                model_name="dwPatientModel",
                model_alias="PatientId",
                model_expr="UPPER(PatientId)",
            )
        ]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        transform_nodes = [n for n in graph.nodes if n.node_type == "transform"]
        assert len(transform_nodes) == 1
        assert "UPPER(PatientId)" in transform_nodes[0].metadata.get("expr", "")

    def test_trace_forward(self):
        entries = [_make_entry()]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        targets = graph.trace_forward("DWH.DimPatient", "PatientId")
        assert any(n.column == "PatientIdentifier" for n in targets)

    def test_trace_backward(self):
        entries = [_make_entry()]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        sources = graph.trace_backward("Patient", "PatientIdentifier")
        assert any(n.column == "PatientId" for n in sources)

    def test_unmapped_targets(self):
        entries = [_make_entry()]  # only PatientIdentifier mapped
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        unmapped = graph.unmapped_targets()
        assert any(n.column == "PatientDateOfBirth" for n in unmapped)

    def test_rejected_entries_excluded(self):
        entries = [_make_entry(status="rejected")]
        graph = LineageBuilder(entries, _mock_manifest(), _mock_o3_model()).build()
        assert len(graph.edges) == 0
