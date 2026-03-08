# tests/etl/test_lineage_report.py
import json
import os
import tempfile
import pytest
from etl.lineage.lineage_builder import LineageGraph, LineageNode, LineageEdge
from etl.lineage.lineage_report import LineageReport


def _make_graph() -> LineageGraph:
    source = LineageNode("source", "DWH.DimPatient", "PatientId", {"data_type": "varchar"})
    target_mapped = LineageNode("target", "Patient", "PatientIdentifier", {"data_type": "String"})
    target_unmapped = LineageNode("target", "Patient", "PatientDateOfBirth", {"data_type": "Date"})
    source_unmapped = LineageNode("source", "DWH.DimPatient", "PatientSSN", {"data_type": "varchar"})
    edge = LineageEdge(source=source, target=target_mapped, confidence=0.92)

    return LineageGraph(
        nodes=[source, target_mapped, target_unmapped, source_unmapped],
        edges=[edge],
    )


class TestCoverageSummary:
    def test_counts(self):
        report = LineageReport(_make_graph())
        summary = report.coverage_summary()
        assert summary["total_o3_attributes"] == 2
        assert summary["mapped"] == 1
        assert summary["unmapped"] == 1
        assert summary["coverage_pct"] == 50.0

    def test_empty_graph(self):
        report = LineageReport(LineageGraph())
        summary = report.coverage_summary()
        assert summary["total_o3_attributes"] == 0
        assert summary["coverage_pct"] == 0.0


class TestToJson:
    def test_writes_valid_json(self):
        report = LineageReport(_make_graph())
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name
        try:
            report.to_json(path)
            with open(path) as f:
                data = json.load(f)
            assert "nodes" in data
            assert "edges" in data
            assert len(data["nodes"]) == 4
            assert len(data["edges"]) == 1
        finally:
            os.unlink(path)


class TestToMarkdown:
    def test_writes_markdown(self):
        report = LineageReport(_make_graph())
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            path = f.name
        try:
            report.to_markdown(path)
            with open(path) as f:
                content = f.read()
            assert "Patient" in content
            assert "PatientIdentifier" in content
            assert "PatientDateOfBirth" in content  # in unmapped section
            assert "|" in content  # table format
        finally:
            os.unlink(path)
