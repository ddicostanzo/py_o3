from helpers.string_helpers import (
    leave_only_letters_numbers_or_underscore,
    leave_letters_numbers_spaces_underscores_dashes,
)


class TestLeaveOnlyLettersNumbersOrUnderscore:
    def test_normal_input(self):
        assert leave_only_letters_numbers_or_underscore("hello_world123") == "hello_world123"

    def test_special_characters_removed(self):
        assert leave_only_letters_numbers_or_underscore("he!lo@wo#ld$") == "helowold"

    def test_empty_string(self):
        assert leave_only_letters_numbers_or_underscore("") == ""

    def test_already_clean(self):
        assert leave_only_letters_numbers_or_underscore("Clean_Input_99") == "Clean_Input_99"

    def test_spaces_removed(self):
        assert leave_only_letters_numbers_or_underscore("hello world") == "helloworld"

    def test_dashes_removed(self):
        assert leave_only_letters_numbers_or_underscore("some-value") == "somevalue"


class TestLeaveLettersNumbersSpacesUnderscoresDashes:
    def test_normal_input(self):
        assert leave_letters_numbers_spaces_underscores_dashes("hello world") == "hello world"

    def test_special_characters_removed(self):
        assert leave_letters_numbers_spaces_underscores_dashes("he!lo@wo#ld") == "helowold"

    def test_empty_string(self):
        assert leave_letters_numbers_spaces_underscores_dashes("") == ""

    def test_already_clean(self):
        assert leave_letters_numbers_spaces_underscores_dashes("Clean_Input-99") == "Clean_Input-99"

    def test_spaces_preserved(self):
        assert leave_letters_numbers_spaces_underscores_dashes("hello world 123") == "hello world 123"

    def test_dashes_preserved(self):
        assert leave_letters_numbers_spaces_underscores_dashes("some-value") == "some-value"

    def test_underscores_preserved(self):
        assert leave_letters_numbers_spaces_underscores_dashes("some_value") == "some_value"


