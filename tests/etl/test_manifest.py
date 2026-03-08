# tests/etl/test_manifest.py
import json
import os
import pytest
from etl.manifest import (
    Column,
    ConceptualModel,
    DWHTable,
    ForeignKey,
    ModelSelect,
    SemanticManifest,
    load_semantic_manifest,
)


SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "src",
    "Resources",
    "semantic_manifest_from_variandw_schema.json",
)
MODELS_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "src",
    "Resources",
    "semantic_manifest_with_models.json",
)


def _make_column_dict(**overrides) -> dict:
    base = {
        "name": "PatientID",
        "dataType": "int",
        "nullable": False,
        "isPrimaryKey": True,
        "isForeignKey": False,
    }
    base.update(overrides)
    return base


def _make_table_dict(**overrides) -> dict:
    base = {
        "schema": "DWH",
        "name": "DimPatient",
        "type": "dimension",
        "primaryKey": ["DimPatientID"],
        "columns": [_make_column_dict()],
        "foreignKeys": [],
    }
    base.update(overrides)
    return base


def _make_select_dict(**overrides) -> dict:
    base = {
        "as": "PatientId",
        "fromTable": "Patient",
        "expr": "PatientId",
        "dataType": "VDT_PATIENTID",
        "tags": "",
    }
    base.update(overrides)
    return base


class TestColumn:
    def test_from_dict(self):
        col = Column.from_dict(_make_column_dict())
        assert col.name == "PatientID"
        assert col.data_type == "int"
        assert col.nullable is False
        assert col.is_primary_key is True

    def test_nullable_column(self):
        col = Column.from_dict(_make_column_dict(nullable=True, isPrimaryKey=False))
        assert col.nullable is True
        assert col.is_primary_key is False


class TestForeignKey:
    def test_from_dict(self):
        data = {
            "fromColumn": "DimPatientID",
            "toTable": "DWH.DimPatient",
            "toColumn": "DimPatientID",
        }
        fk = ForeignKey.from_dict(data)
        assert fk.from_column == "DimPatientID"
        assert fk.to_table == "DWH.DimPatient"
        assert fk.to_column == "DimPatientID"


class TestDWHTable:
    def test_from_dict(self):
        t = DWHTable.from_dict("DWH.DimPatient", _make_table_dict())
        assert t.schema == "DWH"
        assert t.name == "DimPatient"
        assert t.full_name == "DWH.DimPatient"
        assert t.type == "dimension"
        assert len(t.columns) == 1
        assert t.primary_key == ["DimPatientID"]

    def test_with_foreign_keys(self):
        fks = [
            {
                "fromColumn": "DimPatientID",
                "toTable": "DWH.DimPatient",
                "toColumn": "DimPatientID",
            }
        ]
        t = DWHTable.from_dict(
            "DWH.FactActivityBilling",
            _make_table_dict(
                schema="DWH",
                name="FactActivityBilling",
                type="fact",
                foreignKeys=fks,
            ),
        )
        assert t.type == "fact"
        assert len(t.foreign_keys) == 1

    def test_columns_by_name(self):
        t = DWHTable.from_dict("DWH.DimPatient", _make_table_dict())
        assert "PatientID" in t.columns_by_name
        assert t.columns_by_name["PatientID"].data_type == "int"


class TestModelSelect:
    def test_from_dict(self):
        ms = ModelSelect.from_dict(_make_select_dict())
        assert ms.alias == "PatientId"
        assert ms.from_table == "Patient"
        assert ms.expr == "PatientId"
        assert ms.data_type == "VDT_PATIENTID"

    def test_computed_expression(self):
        ms = ModelSelect.from_dict(
            _make_select_dict(
                **{
                    "as": "ActualGantryRtn",
                    "fromTable": "NA",
                    "expr": "[DWH].[vf_ClinicalScaleConversion]('GRtn', ...)",
                }
            )
        )
        assert ms.alias == "ActualGantryRtn"
        assert ms.from_table == "NA"
        assert "vf_ClinicalScaleConversion" in ms.expr


class TestConceptualModel:
    def test_from_dict(self):
        data = {
            "name": "dwActivityBillingModel",
            "baseTable": "DWH.FactActivityBilling",
            "tablesReferenced": ["DWH.FactActivityBilling", "DWH.DimPatient"],
            "joins": [],
            "select": [_make_select_dict()],
        }
        cm = ConceptualModel.from_dict(data)
        assert cm.name == "dwActivityBillingModel"
        assert cm.base_table == "DWH.FactActivityBilling"
        assert len(cm.selects) == 1
        assert cm.selects[0].alias == "PatientId"


class TestLoadSemanticManifest:
    def test_load_from_files(self):
        if not os.path.exists(SCHEMA_PATH) or not os.path.exists(MODELS_PATH):
            pytest.skip("semantic manifest files not found")
        manifest = load_semantic_manifest(SCHEMA_PATH, MODELS_PATH)

        assert manifest.summary["tableCount"] == 352
        assert len(manifest.tables) == 352
        assert len(manifest.models) == 11

        # Verify fact table
        billing = manifest.tables["DWH.FactActivityBilling"]
        assert billing.type == "fact"
        assert len(billing.foreign_keys) > 0

        # Verify model
        billing_model = next(
            m for m in manifest.models if m.name == "dwActivityBillingModel"
        )
        assert billing_model.base_table == "DWH.FactActivityBilling"
        assert len(billing_model.selects) > 0

    def test_fact_dimension_other_counts(self):
        if not os.path.exists(SCHEMA_PATH):
            pytest.skip("schema manifest not found")
        manifest = load_semantic_manifest(SCHEMA_PATH, MODELS_PATH)
        facts = [t for t in manifest.tables.values() if t.type == "fact"]
        dims = [t for t in manifest.tables.values() if t.type == "dimension"]
        others = [t for t in manifest.tables.values() if t.type == "other"]
        assert len(facts) == 40
        assert len(dims) == 96
        assert len(others) == 216
