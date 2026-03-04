from unittest.mock import MagicMock

from base.o3_standard_value import O3StandardValue


def _make_standard_value(item_str):
    """Helper to create an O3StandardValue with mocked parent objects."""
    key_element = MagicMock()
    attribute = MagicMock()
    return O3StandardValue(key_element, attribute, item_str)


class TestO3StandardValueParsing:
    def test_parses_numeric_code(self):
        sv = _make_standard_value("SomeValue {12345}")
        assert sv.numeric_code == "12345"

    def test_parses_value_name(self):
        sv = _make_standard_value("SomeValue {12345}")
        assert sv.value_name == "SomeValue"

    def test_multi_word_value_name(self):
        sv = _make_standard_value("Some Long Value Name {99}")
        assert sv.value_name == "Some Long Value Name"
        assert sv.numeric_code == "99"

    def test_numeric_code_with_semicolon(self):
        sv = _make_standard_value("Value {100; extra}")
        assert sv.numeric_code == "100"

    def test_stores_key_element_reference(self):
        ke = MagicMock()
        attr = MagicMock()
        sv = O3StandardValue(ke, attr, "Val {1}")
        assert sv.key_element is ke

    def test_stores_attribute_reference(self):
        ke = MagicMock()
        attr = MagicMock()
        sv = O3StandardValue(ke, attr, "Val {1}")
        assert sv.attribute is attr

    def test_str_returns_value_name(self):
        sv = _make_standard_value("MyValue {42}")
        assert str(sv) == "MyValue"

    def test_repr_contains_name_and_code(self):
        sv = _make_standard_value("MyValue {42}")
        r = repr(sv)
        assert "MyValue" in r
        assert "42" in r


class TestO3StandardValueEdgeCases:
    def test_no_numeric_code_section(self):
        # When there is no '{' the split behavior means the whole string
        # becomes the "numeric code" and value_name is empty
        sv = _make_standard_value("JustAName")
        # The implementation splits on '{' -- with no '{', split returns
        # a single-element list, so [-1] is the whole string and [:-1] is empty
        assert sv.value_name == ""
        assert sv.numeric_code == "JustAName"

    def test_empty_braces(self):
        sv = _make_standard_value("Value {}")
        assert sv.value_name == "Value"
        assert sv.numeric_code == ""

    def test_multiple_braces(self):
        sv = _make_standard_value("A {B} extra {C}")
        # split('{') -> ['A ', 'B} extra ', 'C}']
        # numeric_code = 'C}' -> replace('{','').replace('}','') -> 'C'
        # value_name = 'A ' + 'B} extra ' joined with space stripping
        assert sv.numeric_code == "C"
