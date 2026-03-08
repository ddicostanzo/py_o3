# tests/etl/test_match_engine.py
import pytest
from etl.mapping.match_engine import MatchCandidate, MatchEngine


class TestNameSimilarity:
    def setup_method(self):
        self.engine = MatchEngine()

    def test_exact_match(self):
        score = self.engine._name_similarity("PatientId", "PatientId")
        assert score == 1.0

    def test_case_insensitive(self):
        score = self.engine._name_similarity("patientid", "PatientId")
        assert score == 1.0

    def test_prefix_stripping(self):
        score = self.engine._name_similarity("DimPatientID", "PatientIdentifier")
        assert score > 0.5

    def test_no_overlap(self):
        score = self.engine._name_similarity("GantryRotation", "BillingCode")
        assert score < 0.3

    def test_partial_token_overlap(self):
        score = self.engine._name_similarity("PatientFirstName", "PatientName")
        assert score > 0.5


class TestTypeCompatibility:
    def setup_method(self):
        self.engine = MatchEngine()

    def test_string_types_compatible(self):
        score = self.engine._type_compatibility("varchar", "String")
        assert score > 0.8

    def test_int_types_compatible(self):
        score = self.engine._type_compatibility("int", "Integer")
        assert score > 0.8

    def test_vdt_type_compatible(self):
        score = self.engine._type_compatibility("VDT_PATIENTID", "String")
        assert score > 0.5

    def test_incompatible_types(self):
        score = self.engine._type_compatibility("datetime", "Boolean")
        assert score < 0.3


class TestScore:
    def setup_method(self):
        self.engine = MatchEngine()

    def test_high_confidence_match(self):
        candidate = self.engine.score(
            dwh_name="PatientId",
            dwh_type="VDT_PATIENTID",
            o3_name="PatientIdentifier",
            o3_type="String",
        )
        assert isinstance(candidate, MatchCandidate)
        assert candidate.score >= 0.5
        assert "name" in candidate.signals
        assert "type" in candidate.signals

    def test_low_confidence_mismatch(self):
        candidate = self.engine.score(
            dwh_name="CollRtnOverrideFlag",
            dwh_type="VDT_OVERRIDEFLAG",
            o3_name="PatientIdentifier",
            o3_type="String",
        )
        assert candidate.score < 0.5

    def test_context_bonus(self):
        base = self.engine.score(
            dwh_name="DateOfBirth",
            dwh_type="datetime",
            o3_name="PatientDateOfBirth",
            o3_type="Date",
        )
        boosted = self.engine.score(
            dwh_name="DateOfBirth",
            dwh_type="datetime",
            o3_name="PatientDateOfBirth",
            o3_type="Date",
            dwh_context="Patient",
            o3_context="Patient",
        )
        assert boosted.score >= base.score
