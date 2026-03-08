# tests/etl/test_crosswalk.py
from unittest.mock import MagicMock, PropertyMock
import pytest
from etl.mapping.crosswalk import Crosswalk
from etl.mapping.mapping_store import CrosswalkEntry


def _mock_o3_model():
    """Mock O3DataModel with two key elements, each with attributes."""
    model = MagicMock()

    # Patient key element
    patient_ke = MagicMock()
    patient_ke.key_element_name = "Patient"
    patient_ke.string_code = "KEL_Patient"

    patient_attr = MagicMock()
    patient_attr.value_name = "PatientIdentifier"
    patient_attr.value_data_type = "String"
    patient_ke.list_attributes = [patient_attr]

    # Course key element
    course_ke = MagicMock()
    course_ke.key_element_name = "Course"
    course_ke.string_code = "KEL_Course"

    course_attr = MagicMock()
    course_attr.value_name = "CourseIdentifier"
    course_attr.value_data_type = "String"
    course_ke.list_attributes = [course_attr]

    model.key_elements = {
        "Patient": patient_ke,
        "Course": course_ke,
    }
    return model


def _mock_manifest():
    """Mock SemanticManifest with one table and one model."""
    manifest = MagicMock()

    # DWH table
    col = MagicMock()
    col.name = "PatientId"
    col.data_type = "varchar"

    table = MagicMock()
    table.full_name = "DWH.DimPatient"
    table.name = "DimPatient"
    table.type = "dimension"
    table.columns = [col]

    manifest.tables = {"DWH.DimPatient": table}

    # Conceptual model
    select = MagicMock()
    select.alias = "PatientId"
    select.from_table = "Patient"
    select.expr = "PatientId"
    select.data_type = "VDT_PATIENTID"

    model = MagicMock()
    model.name = "dwPatientModel"
    model.base_table = "DWH.FactPatientPayor"
    model.selects = [select]

    manifest.models = [model]
    return manifest


def _mock_registry():
    """Mock ModelRegistry with one entry point."""
    registry = MagicMock()
    ep = MagicMock()
    ep.base_table = "DWH.FactPatientPayor"
    ep.preferred_conceptual_model = "dwPatientModel"
    registry.entry_points = {"patient": ep}
    registry.field_policy_defaults = MagicMock()
    registry.field_policy_defaults.deny_list = ["PatientSSN"]
    return registry


class TestGenerateSuggestions:
    def test_returns_crosswalk_entries(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        assert len(suggestions) > 0
        assert all(isinstance(s, CrosswalkEntry) for s in suggestions)

    def test_suggestions_have_auto_status(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        assert all(s.status == "auto" for s in suggestions)

    def test_denied_columns_excluded(self):
        manifest = _mock_manifest()
        ssn_col = MagicMock()
        ssn_col.name = "PatientSSN"
        ssn_col.data_type = "varchar"
        manifest.tables["DWH.DimPatient"].columns.append(ssn_col)

        cw = Crosswalk(manifest, _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        assert not any(s.dwh_column == "PatientSSN" for s in suggestions)

    def test_suggestions_sorted_by_confidence(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())
        suggestions = cw.generate_suggestions()
        scores = [s.confidence for s in suggestions]
        assert scores == sorted(scores, reverse=True)


class TestMerge:
    def test_curated_overrides_suggestions(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())

        suggestions = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="auto",
            )
        ]
        curated = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="confirmed",
            )
        ]

        merged = cw.merge(suggestions, curated)
        assert len(merged) == 1
        assert merged[0].status == "confirmed"

    def test_rejected_entries_excluded_from_active(self):
        cw = Crosswalk(_mock_manifest(), _mock_registry(), _mock_o3_model())

        suggestions = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="auto",
            )
        ]
        curated = [
            CrosswalkEntry(
                dwh_table="DWH.DimPatient", dwh_column="PatientId",
                model_name=None, model_alias=None, model_expr=None,
                o3_key_element="Patient", o3_attribute="PatientIdentifier",
                confidence=0.9, status="rejected",
            )
        ]

        merged = cw.merge(suggestions, curated)
        assert merged[0].status == "rejected"
        assert not merged[0].is_active
