# tests/etl/test_mapping_store.py
import json
import os
import tempfile
import pytest
from etl.mapping.mapping_store import CrosswalkEntry, MappingStore


def _make_entry(**overrides) -> CrosswalkEntry:
    base = {
        "dwh_table": "DWH.DimPatient",
        "dwh_column": "PatientId",
        "model_name": "dwPatientModel",
        "model_alias": "PatientId",
        "model_expr": "PatientId",
        "o3_key_element": "Patient",
        "o3_attribute": "PatientIdentifier",
        "confidence": 0.92,
        "status": "auto",
    }
    base.update(overrides)
    return CrosswalkEntry(**base)


class TestCrosswalkEntry:
    def test_to_dict_roundtrip(self):
        entry = _make_entry()
        d = entry.to_dict()
        restored = CrosswalkEntry.from_dict(d)
        assert restored == entry

    def test_is_confirmed(self):
        assert _make_entry(status="confirmed").is_active
        assert _make_entry(status="auto").is_active
        assert _make_entry(status="manual").is_active
        assert not _make_entry(status="rejected").is_active


class TestMappingStore:
    def test_save_and_load(self):
        entries = [_make_entry(), _make_entry(dwh_column="DateOfBirth", o3_attribute="PatientDateOfBirth")]
        store = MappingStore()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name

        try:
            store.save(entries, path)
            loaded = store.load(path)
            assert len(loaded) == 2
            assert loaded[0].dwh_column == "PatientId"
            assert loaded[1].dwh_column == "DateOfBirth"
        finally:
            os.unlink(path)

    def test_load_empty_file(self):
        store = MappingStore()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([], f)
            path = f.name
        try:
            loaded = store.load(path)
            assert loaded == []
        finally:
            os.unlink(path)

    def test_diff(self):
        old = [_make_entry(confidence=0.9)]
        new = [
            _make_entry(confidence=0.95),
            _make_entry(dwh_column="DateOfBirth", o3_attribute="DOB"),
        ]
        store = MappingStore()
        diff = store.diff(old, new)
        assert len(diff.added) == 1
        assert len(diff.changed) == 1
        assert len(diff.removed) == 0
