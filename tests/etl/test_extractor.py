# tests/etl/test_extractor.py
from unittest.mock import MagicMock
import pytest
from etl.pipeline.extractor import ExtractQuery, Extractor
from etl.mapping.mapping_store import CrosswalkEntry
from etl.registry import (
    DateBasis, EntryPoint, FieldPolicy, GlobalPolicy, DateRangePolicy,
    JoinPolicy, JoinSpec, ModelConfig, ModelRegistry, QuerySafety, TimePolicy,
)
from etl.manifest import SemanticManifest, DWHTable, Column, ConceptualModel, ModelSelect


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


def _make_registry() -> ModelRegistry:
    return ModelRegistry(
        entry_points={
            "billing": EntryPoint(
                base_table="DWH.FactActivityBilling",
                preferred_conceptual_model="dwActivityBillingModel",
                time_policy=TimePolicy(
                    default_date_key="DimDateID_FromDateOfService",
                    date_basis=DateBasis(
                        enum=["service"],
                        map={"service": "DimDateID_FromDateOfService"},
                        default="service",
                    ),
                    default_lookback_days=90,
                ),
            )
        },
        models={
            "dwActivityBillingModel": ModelConfig(
                base_table="DWH.FactActivityBilling",
                join_policy=JoinPolicy(
                    mode="facts-first",
                    allowed_dimension_joins=[
                        JoinSpec("DWH.DimPatient", "DimPatientID", "DimPatientID"),
                    ],
                ),
                time_policy=TimePolicy(
                    default_date_key="DimDateID_FromDateOfService",
                    date_key_candidates=["DimDateID_FromDateOfService"],
                    date_basis=DateBasis(
                        enum=["service"],
                        map={"service": "DimDateID_FromDateOfService"},
                        default="service",
                    ),
                ),
                field_policy=FieldPolicy(deny_list=["PatientSSN"]),
            )
        },
        global_policy=GlobalPolicy(
            timezone="America/New_York",
            date_range=DateRangePolicy("inclusive", ["inclusive"]),
            query_safety=QuerySafety(
                select_only=True,
                default_row_limit=1000,
                max_row_limit=100000,
                require_date_filter_for_tables=["DWH.FactActivityBilling"],
                cross_fact_joins="disallow_unless_bridge",
            ),
        ),
        field_policy_defaults=FieldPolicy(deny_list=["PatientSSN"]),
    )


def _make_manifest() -> MagicMock:
    manifest = MagicMock(spec=SemanticManifest)
    col = MagicMock(spec=Column)
    col.name = "DimPatientID"
    col.data_type = "int"
    table = MagicMock(spec=DWHTable)
    table.full_name = "DWH.FactActivityBilling"
    table.columns = [col]
    table.columns_by_name = {"DimPatientID": col}
    manifest.tables = {"DWH.FactActivityBilling": table}
    manifest.models = []
    manifest.models_by_name = {}
    return manifest


class TestGenerateQuery:
    def test_returns_extract_query(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert isinstance(query, ExtractQuery)
        assert query.entry_point == "billing"
        assert query.base_table == "DWH.FactActivityBilling"
        assert "SELECT" in query.sql
        assert "FROM" in query.sql

    def test_includes_date_filter(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "DimDateID_FromDateOfService" in query.sql
        assert "WHERE" in query.sql

    def test_includes_row_limit(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "TOP" in query.sql or "LIMIT" in query.sql

    def test_custom_lookback_days(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing", lookback_days=30)
        assert "30" in query.sql

    def test_denied_columns_excluded(self):
        entries = [_make_entry(dwh_column="PatientSSN", model_alias="PatientSSN")]
        extractor = Extractor(entries, _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "PatientSSN" not in query.sql

    def test_invalid_entry_point_raises(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        with pytest.raises(ValueError, match="entry point"):
            extractor.generate_query("nonexistent")

    def test_includes_join_for_dimension(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        query = extractor.generate_query("billing")
        assert "JOIN" in query.sql or "DimPatient" in query.sql


class TestGenerateAllQueries:
    def test_returns_queries_for_all_entry_points(self):
        extractor = Extractor([_make_entry()], _make_manifest(), _make_registry())
        queries = extractor.generate_all_queries()
        assert len(queries) == 1
        assert queries[0].entry_point == "billing"
