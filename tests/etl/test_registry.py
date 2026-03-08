# tests/etl/test_registry.py
import json
import os
import pytest
from etl.registry import (
    DateBasis,
    EntryPoint,
    FieldPolicy,
    JoinPolicy,
    JoinSpec,
    ModelConfig,
    ModelRegistry,
    QuerySafety,
    TimePolicy,
    load_model_registry,
)


REGISTRY_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "src", "Resources", "model_registry.json"
)


def _make_minimal_registry() -> dict:
    """Minimal valid model_registry.json structure."""
    return {
        "version": "1.0",
        "entryPoints": {
            "billing": {
                "baseTable": "DWH.FactActivityBilling",
                "preferredConceptualModel": "dwActivityBillingModel",
                "timePolicy": {
                    "defaultDateKey": "DimDateID_FromDateOfService",
                    "dateBasis": {
                        "enum": ["service"],
                        "map": {"service": "DimDateID_FromDateOfService"},
                        "default": "service",
                    },
                    "defaultLookbackDays": 90,
                },
            }
        },
        "models": {
            "dwActivityBillingModel": {
                "baseTable": "DWH.FactActivityBilling",
                "joinPolicy": {
                    "mode": "facts-first",
                    "allowedDimensionJoins": [
                        {
                            "table": "DWH.DimPatient",
                            "fromColumn": "DimPatientID",
                            "toColumn": "DimPatientID",
                        }
                    ],
                },
                "timePolicy": {
                    "dateKeyCandidates": ["DimDateID_FromDateOfService"],
                    "defaultDateKey": "DimDateID_FromDateOfService",
                    "dateBasis": {
                        "enum": ["service"],
                        "map": {"service": "DimDateID_FromDateOfService"},
                        "default": "service",
                    },
                },
                "fieldPolicy": {"denyList": ["PatientSSN"]},
            }
        },
        "globalPolicy": {
            "timezone": "America/New_York",
            "dateRange": {
                "defaultMode": "inclusive",
                "supportedModes": ["inclusive", "exclusive"],
                "notes": [],
            },
            "querySafety": {
                "selectOnly": True,
                "defaultRowLimit": 1000,
                "maxRowLimit": 100000,
                "requireDateFilterForTables": ["DWH.FactActivityBilling"],
                "crossFactJoins": "disallow_unless_bridge",
                "notes": [],
            },
        },
        "fieldPolicyDefaults": {"denyList": ["PatientSSN"], "notes": []},
    }


class TestDateBasis:
    def test_from_dict(self):
        data = {
            "enum": ["service", "completed"],
            "map": {
                "service": "DimDateID_FromDateOfService",
                "completed": "DimDateID_CompletedDateTime",
            },
            "default": "service",
        }
        db = DateBasis.from_dict(data)
        assert db.enum == ["service", "completed"]
        assert db.map["service"] == "DimDateID_FromDateOfService"
        assert db.default == "service"

    def test_resolve_returns_mapped_key(self):
        db = DateBasis(
            enum=["service"],
            map={"service": "DimDateID_FromDateOfService"},
            default="service",
        )
        assert db.resolve("service") == "DimDateID_FromDateOfService"

    def test_resolve_invalid_raises(self):
        db = DateBasis(enum=["service"], map={"service": "X"}, default="service")
        with pytest.raises(ValueError, match="invalid"):
            db.resolve("nonexistent")


class TestTimePolicy:
    def test_from_entry_point_dict(self):
        data = {
            "defaultDateKey": "DimDateID_FromDateOfService",
            "dateBasis": {
                "enum": ["service"],
                "map": {"service": "DimDateID_FromDateOfService"},
                "default": "service",
            },
            "defaultLookbackDays": 90,
        }
        tp = TimePolicy.from_dict(data)
        assert tp.default_date_key == "DimDateID_FromDateOfService"
        assert tp.default_lookback_days == 90
        assert tp.date_basis is not None
        assert tp.date_basis.default == "service"

    def test_from_model_dict_with_candidates(self):
        data = {
            "dateKeyCandidates": ["DimDateID_FromDateOfService"],
            "defaultDateKey": "DimDateID_FromDateOfService",
            "dateBasis": {
                "enum": ["service"],
                "map": {"service": "DimDateID_FromDateOfService"},
                "default": "service",
            },
        }
        tp = TimePolicy.from_dict(data)
        assert tp.date_key_candidates == ["DimDateID_FromDateOfService"]

    def test_null_date_key(self):
        data = {"dateKeyCandidates": [], "defaultDateKey": None}
        tp = TimePolicy.from_dict(data)
        assert tp.default_date_key is None
        assert tp.date_basis is None


class TestJoinSpec:
    def test_from_dict(self):
        data = {
            "table": "DWH.DimPatient",
            "fromColumn": "DimPatientID",
            "toColumn": "DimPatientID",
        }
        js = JoinSpec.from_dict(data)
        assert js.table == "DWH.DimPatient"
        assert js.from_column == "DimPatientID"
        assert js.to_column == "DimPatientID"


class TestLoadModelRegistry:
    def test_load_from_minimal_dict(self):
        data = _make_minimal_registry()
        registry = ModelRegistry.from_dict(data)

        assert "billing" in registry.entry_points
        ep = registry.entry_points["billing"]
        assert ep.base_table == "DWH.FactActivityBilling"
        assert ep.preferred_conceptual_model == "dwActivityBillingModel"
        assert ep.time_policy.default_lookback_days == 90

        assert "dwActivityBillingModel" in registry.models
        model = registry.models["dwActivityBillingModel"]
        assert model.base_table == "DWH.FactActivityBilling"
        assert model.join_policy.mode == "facts-first"
        assert len(model.join_policy.allowed_dimension_joins) == 1
        assert model.field_policy.deny_list == ["PatientSSN"]

        assert registry.global_policy.query_safety.select_only is True
        assert registry.global_policy.query_safety.default_row_limit == 1000

    def test_load_from_file(self):
        if not os.path.exists(REGISTRY_PATH):
            pytest.skip("model_registry.json not found")
        registry = load_model_registry(REGISTRY_PATH)

        assert len(registry.entry_points) == 4
        assert "billing" in registry.entry_points
        assert "scheduling" in registry.entry_points
        assert "activity" in registry.entry_points
        assert "treatment_history" in registry.entry_points

        assert len(registry.models) == 11
        assert registry.global_policy.query_safety.max_row_limit == 100000

    def test_field_policy_defaults_loaded(self):
        data = _make_minimal_registry()
        registry = ModelRegistry.from_dict(data)
        assert "PatientSSN" in registry.field_policy_defaults.deny_list
