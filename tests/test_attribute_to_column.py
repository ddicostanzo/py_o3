"""Tests for AttributeToSQLColumn class."""

import warnings
from unittest.mock import MagicMock

import pytest

from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.attribute_to_column import (
    AttributeToSQLColumn,
)


def _make_attribute(string_code="Patient_Name", value_name="Name", value_data_type="String",
                    standard_values_list=None, reference_system="", allow_null="Yes",
                    key_element_string_code="Patient"):
    attr = MagicMock()
    attr.string_code = string_code
    attr.value_name = value_name
    attr.value_data_type = value_data_type
    attr.standard_values_list = standard_values_list or []
    attr.reference_system_for_values = reference_system if reference_system else None
    attr.allow_null_values = allow_null
    ke = MagicMock()
    ke.string_code = key_element_string_code
    attr.key_element = ke
    return attr


# ---------------------------------------------------------------------------
# Data type resolution tests
# ---------------------------------------------------------------------------
class TestSetDateForISO8601:
    """ISO 8601 reference system should produce Date type."""

    def test_iso8601_sets_date_type(self):
        attr = _make_attribute(reference_system="ISO 8601", value_data_type="String")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Date"

    def test_iso8601_partial_match(self):
        attr = _make_attribute(reference_system="ISO 8601 date format", value_data_type="String")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Date"


class TestSetStandardValuesDataType:
    """Standard values list present should produce String type."""

    def test_standard_values_sets_string(self):
        attr = _make_attribute(value_data_type="Integer", standard_values_list=["A", "B"])
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        # ISO 8601 won't match (no ref system), standard values comes second
        # but standard values should override since it comes before integer in the chain
        # Actually: the chain runs in order and stops at first match.
        # __set_date_for_iso8601 -> no match (ref is None)
        # __set_standard_values_data_type -> match (list is non-empty) -> "String"
        assert col.column_data_type == "String"


class TestSetEmptyValueTypes:
    """Empty value data type should produce String type."""

    def test_empty_value_data_type_sets_string(self):
        attr = _make_attribute(value_data_type="")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "String"


class TestSetStringDataType:
    """String value data type should produce String type."""

    def test_string_data_type(self):
        attr = _make_attribute(value_data_type="String")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "String"

    def test_string_case_insensitive(self):
        attr = _make_attribute(value_data_type="string")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        assert col.column_data_type == "String"


class TestSetIntDataType:
    """Int/Integer value data type should produce Integer type."""

    def test_integer_data_type(self):
        attr = _make_attribute(value_data_type="Integer")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Integer"

    def test_int_data_type(self):
        attr = _make_attribute(value_data_type="Int")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Integer"


class TestSetDecimalDataType:
    """Decimal/Numeric value data type should produce Decimal type."""

    def test_decimal_data_type(self):
        attr = _make_attribute(value_data_type="Decimal")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Decimal"

    def test_numeric_data_type(self):
        attr = _make_attribute(value_data_type="Numeric")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        assert col.column_data_type == "Decimal"


class TestSetBoolDataType:
    """Boolean value data type should produce Boolean type."""

    def test_boolean_data_type(self):
        attr = _make_attribute(value_data_type="Boolean")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Boolean"

    def test_boolean_case_insensitive(self):
        attr = _make_attribute(value_data_type="boolean")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        assert col.column_data_type == "Boolean"


class TestSetBinaryDataType:
    """DICOM value data type should produce Binary type."""

    def test_dicom_data_type(self):
        attr = _make_attribute(value_data_type="DICOM")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Binary"

    def test_dicom_case_insensitive(self):
        attr = _make_attribute(value_data_type="dicom image")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        assert col.column_data_type == "Binary"


class TestSetDateDataType:
    """Date in value name or value_data_type should produce Date type."""

    def test_date_in_value_name(self):
        attr = _make_attribute(value_name="Birth Date", value_data_type="SomethingUnknown")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Date"

    def test_date_value_data_type(self):
        attr = _make_attribute(value_data_type="Date")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_data_type == "Date"


# ---------------------------------------------------------------------------
# Nullable logic tests
# ---------------------------------------------------------------------------
class TestNullableAlwaysNullable:
    """Attributes with allow_null in _ALWAYS_NULLABLE should be NULL."""

    def test_yes_is_null(self):
        attr = _make_attribute(allow_null="Yes")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"

    def test_true_bool_is_null(self):
        attr = _make_attribute(allow_null=True)
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"

    def test_secondary_cancer_is_null(self):
        attr = _make_attribute(allow_null="Yes, if diagnosis is for secondary cancer")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"


class TestNullableAlwaysNotNull:
    """Attributes with allow_null 'No' should be NOT NULL."""

    def test_no_is_not_null(self):
        attr = _make_attribute(allow_null="No")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NOT NULL"


class TestNullablePHIDependent:
    """PHI-dependent nullable logic."""

    def test_alloing_typo_phi_allowed(self):
        attr = _make_attribute(
            allow_null="No for systems alloing PHI. Yes for systems not allowing PHI"
        )
        col = AttributeToSQLColumn(attr, True, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NOT NULL"

    def test_alloing_typo_phi_not_allowed(self):
        attr = _make_attribute(
            allow_null="No for systems alloing PHI. Yes for systems not allowing PHI"
        )
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"

    def test_allowing_phi_allowed(self):
        attr = _make_attribute(
            allow_null="No for systems allowing PHI. Yes for systems not allowing PHI"
        )
        col = AttributeToSQLColumn(attr, True, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NOT NULL"

    def test_allowing_phi_not_allowed(self):
        attr = _make_attribute(
            allow_null="No for systems allowing PHI. Yes for systems not allowing PHI"
        )
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"

    def test_reverse_phi_dependent_phi_allowed(self):
        attr = _make_attribute(
            allow_null="Yes for systems allowing PHI. No for systems not allowing PHI"
        )
        col = AttributeToSQLColumn(attr, True, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"

    def test_reverse_phi_dependent_phi_not_allowed(self):
        attr = _make_attribute(
            allow_null="Yes for systems allowing PHI. No for systems not allowing PHI"
        )
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NOT NULL"


class TestNullableMRNOverride:
    """Patient_MRN nullable flips based on PHI."""

    def test_mrn_phi_allowed_not_null(self):
        attr = _make_attribute(string_code="Patient_MRN", allow_null="Yes")
        col = AttributeToSQLColumn(attr, True, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NOT NULL"

    def test_mrn_phi_not_allowed_null(self):
        attr = _make_attribute(string_code="Patient_MRN", allow_null="Yes")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"


class TestNullableAnonPatIDOverride:
    """Patient_AnonPatID nullable flips based on PHI (opposite of MRN)."""

    def test_anonpatid_phi_allowed_null(self):
        attr = _make_attribute(string_code="Patient_AnonPatID", allow_null="No")
        col = AttributeToSQLColumn(attr, True, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NULL"

    def test_anonpatid_phi_not_allowed_not_null(self):
        attr = _make_attribute(string_code="Patient_AnonPatID", allow_null="No")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_nullable == "NOT NULL"


class TestNullableFallbackWarning:
    """Unknown allow_null value should warn and default to NULL."""

    def test_unknown_allow_null_warns(self):
        attr = _make_attribute(allow_null="SomeUnknownValue")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
            null_warnings = [x for x in w if "Defaulting to NULL" in str(x.message)]
            assert len(null_warnings) == 1
        assert col.column_nullable == "NULL"


# ---------------------------------------------------------------------------
# Column name tests
# ---------------------------------------------------------------------------
class TestColumnName:
    """Tests for column_name property."""

    def test_column_name_without_standard_values(self):
        attr = _make_attribute(string_code="Patient_Name")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_name == "Name"

    def test_column_name_with_standard_values(self):
        attr = _make_attribute(string_code="Patient_Gender", standard_values_list=["M", "F"])
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_name == "GenderId"

    def test_column_name_multi_segment(self):
        attr = _make_attribute(string_code="Diagnosis_Primary_Site")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_name == "PrimarySite"

    def test_column_name_with_special_chars_stripped(self):
        # string_code with special chars after split should be cleaned
        attr = _make_attribute(string_code="Patient_Some-Thing")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert col.column_name == "SomeThing"


# ---------------------------------------------------------------------------
# Column creation text tests
# ---------------------------------------------------------------------------
class TestColumnCreationText:
    """Tests for column_creation_text property."""

    def test_creation_text_format_mssql(self):
        attr = _make_attribute(value_data_type="String", allow_null="Yes")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        text = col.column_creation_text
        assert "varchar(max)" in text
        assert "NULL" in text
        assert col.column_name in text

    def test_creation_text_format_psql(self):
        attr = _make_attribute(value_data_type="Integer", allow_null="No")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        text = col.column_creation_text
        assert "integer" in text
        assert "NOT NULL" in text

    def test_creation_text_boolean_mssql(self):
        attr = _make_attribute(value_data_type="Boolean", allow_null="Yes")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert "bit" in col.column_creation_text

    def test_creation_text_boolean_psql(self):
        attr = _make_attribute(value_data_type="Boolean", allow_null="Yes")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        assert "boolean" in col.column_creation_text

    def test_creation_text_decimal_mssql(self):
        attr = _make_attribute(value_data_type="Decimal", allow_null="No")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert "decimal" in col.column_creation_text

    def test_creation_text_decimal_psql(self):
        attr = _make_attribute(value_data_type="Decimal", allow_null="No")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        assert "numeric" in col.column_creation_text

    def test_creation_text_date_mssql(self):
        attr = _make_attribute(value_data_type="Date", allow_null="Yes")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)
        assert "datetime2" in col.column_creation_text

    def test_creation_text_date_psql(self):
        attr = _make_attribute(value_data_type="Date", allow_null="Yes")
        col = AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)
        assert "timestamptz" in col.column_creation_text


# ---------------------------------------------------------------------------
# Unsupported SQL server type
# ---------------------------------------------------------------------------
class TestUnresolvableDataType:
    """Completely unrecognizable data type should raise ValueError."""

    def test_unresolvable_data_type_raises(self):
        attr = _make_attribute(
            value_data_type="ComplexNumber",
            value_name="SomeField",
            standard_values_list=[],
            reference_system="",
        )
        with pytest.raises(ValueError, match="Could not determine SQL data type"):
            AttributeToSQLColumn(attr, False, SupportedSQLServers.MSSQL)

    def test_error_message_includes_attribute_name(self):
        attr = _make_attribute(
            value_data_type="Quaternion",
            value_name="RotationField",
            standard_values_list=[],
            reference_system="",
        )
        with pytest.raises(ValueError, match="RotationField"):
            AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)

    def test_error_message_includes_data_type(self):
        attr = _make_attribute(
            value_data_type="Quaternion",
            value_name="RotationField",
            standard_values_list=[],
            reference_system="",
        )
        with pytest.raises(ValueError, match="Quaternion"):
            AttributeToSQLColumn(attr, False, SupportedSQLServers.PSQL)


class TestUnsupportedSQLServer:
    """Unsupported SQL server type should raise an error."""

    def test_invalid_server_type_raises(self):
        attr = _make_attribute()
        with pytest.raises(ValueError):
            AttributeToSQLColumn(attr, False, "INVALID")
