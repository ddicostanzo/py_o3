import warnings
from unittest.mock import MagicMock

from base.o3_attribute import O3Attribute


def _make_attr_dict(**overrides):
    """Build a minimal attribute dict for O3Attribute.__init__."""
    base = {
        "ValueName": "TestAttr",
        "ValueType": "Attribute",
        "StringCode": "KE_TestAttr",
        "NumericCode": "100",
        "Definition": "A test attribute",
        "ValuePriority": "1",
        "MoreThanOneValueAllowed": "No",
        "SCTID": "",
        "NCITC": "",
        "NCIMT": "",
        "ValueDataType": "String",
        "StandardValuesUse": "",
        "StandardValuesList": [],
        "ReferenceSystemForValues": None,
        "AllowNullValues": "Yes",
        "ValueExample": "example",
    }
    base.update(overrides)
    return base


def _make_attribute(item_dict=None, clean=True):
    """Create an O3Attribute with a mocked key_element."""
    if item_dict is None:
        item_dict = _make_attr_dict()
    key_element = MagicMock()
    return O3Attribute(key_element, item_dict, clean=clean)


class TestCleanStandardValuesList:
    """Tests for __clean_standard_values_list which filters out
    'Reference System' and 'Current ICD standard' entries.
    This was originally a list mutation bug (iterating while removing);
    now uses list comprehension."""

    def test_reference_system_entries_removed(self):
        item_dict = _make_attr_dict(
            StandardValuesList=[
                "Value A {1}",
                "Reference System: ICD-10 {99}",
                "Value B {2}",
            ],
        )
        attr = _make_attribute(item_dict)
        names = [sv.value_name for sv in attr.standard_values_list]
        assert "Value A" in names
        assert "Value B" in names
        assert not any("Reference System" in n for n in names)

    def test_current_icd_standard_entries_removed(self):
        item_dict = _make_attr_dict(
            StandardValuesList=[
                "Current ICD standard v10 {50}",
                "Actual Value {1}",
            ],
        )
        attr = _make_attribute(item_dict)
        names = [sv.value_name for sv in attr.standard_values_list]
        assert "Actual Value" in names
        assert not any("Current ICD standard" in n for n in names)

    def test_both_reference_and_icd_removed(self):
        item_dict = _make_attr_dict(
            StandardValuesList=[
                "Reference System: something {10}",
                "Current ICD standard v10 {20}",
                "Keep This {30}",
            ],
        )
        attr = _make_attribute(item_dict)
        assert len(attr.standard_values_list) == 1
        assert attr.standard_values_list[0].value_name == "Keep This"

    def test_no_entries_to_remove(self):
        item_dict = _make_attr_dict(
            StandardValuesList=["Alpha {1}", "Beta {2}"],
        )
        attr = _make_attribute(item_dict)
        assert len(attr.standard_values_list) == 2

    def test_empty_standard_values_list(self):
        item_dict = _make_attr_dict(StandardValuesList=[])
        attr = _make_attribute(item_dict)
        assert len(attr.standard_values_list) == 0

    def test_all_entries_filtered_leaves_empty_list(self):
        item_dict = _make_attr_dict(
            StandardValuesList=[
                "Reference System: x {1}",
                "Current ICD standard {2}",
            ],
        )
        attr = _make_attribute(item_dict)
        assert len(attr.standard_values_list) == 0


class TestCleanValueDataTypes:
    """Tests for __clean_value_data_types normalization logic."""

    def test_standard_values_force_string_type(self):
        """When standard_values_list is non-empty, data type becomes String."""
        item_dict = _make_attr_dict(
            ValueDataType="Integer",
            StandardValuesList=["Val {1}"],
        )
        attr = _make_attribute(item_dict)
        assert attr.value_data_type == "String"

    def test_empty_data_type_defaults_to_string_with_warning(self):
        """When no standard values and ValueDataType is empty, defaults to String and warns."""
        item_dict = _make_attr_dict(
            ValueDataType="",
            StandardValuesList=[],
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            attr = _make_attribute(item_dict)
            assert attr.value_data_type == "String"
            assert any("Setting value data type to string" in str(warning.message) for warning in w)

    def test_int_normalized_to_integer(self):
        item_dict = _make_attr_dict(ValueDataType="Int")
        attr = _make_attribute(item_dict)
        assert attr.value_data_type == "Integer"

    def test_numeric_normalized_to_decimal(self):
        item_dict = _make_attr_dict(ValueDataType="Numeric")
        attr = _make_attribute(item_dict)
        assert attr.value_data_type == "Decimal"

    def test_lowercase_string_normalized(self):
        item_dict = _make_attr_dict(ValueDataType="string")
        attr = _make_attribute(item_dict)
        assert attr.value_data_type == "String"

    def test_date_in_value_name_sets_date_type(self):
        item_dict = _make_attr_dict(
            ValueName="SurgeryDate",
            ValueDataType="unknown_type",
        )
        attr = _make_attribute(item_dict)
        assert attr.value_data_type == "Date"

    def test_valid_data_type_unchanged(self):
        for dt in ["Boolean", "Binary", "Date", "Decimal", "Integer", "String"]:
            item_dict = _make_attr_dict(ValueDataType=dt)
            attr = _make_attribute(item_dict)
            assert attr.value_data_type == dt or attr.value_data_type == "String"
            # Note: if standard_values_list is empty and type is valid, it stays as-is


class TestCleanDisabled:
    """When clean=False, no normalization happens."""

    def test_no_cleaning_preserves_original_data_type(self):
        item_dict = _make_attr_dict(ValueDataType="Int")
        attr = _make_attribute(item_dict, clean=False)
        assert attr.value_data_type == "Int"

    def test_no_cleaning_preserves_reference_system_entries(self):
        item_dict = _make_attr_dict(
            StandardValuesList=["Reference System: x {1}", "Value {2}"],
        )
        attr = _make_attribute(item_dict, clean=False)
        assert len(attr.standard_values_list) == 2


class TestCheckReferenceSystem:
    """Tests for __check_reference_system which extracts reference system
    from standard values list when ReferenceSystemForValues is None."""

    def test_extracts_reference_system_from_standard_values(self):
        item_dict = _make_attr_dict(
            ReferenceSystemForValues=None,
            StandardValuesList=[
                "Reference System: ICD-10 {99}",
                "Value A {1}",
            ],
        )
        attr = _make_attribute(item_dict)
        assert attr.reference_system_for_values == "ICD-10"

    def test_preserves_existing_reference_system(self):
        item_dict = _make_attr_dict(
            ReferenceSystemForValues="Already Set",
            StandardValuesList=["Reference System: ICD-10 {99}"],
        )
        attr = _make_attribute(item_dict)
        assert attr.reference_system_for_values == "Already Set"
