"""Regex-based sanitization functions for SQL identifier strings."""
import re

_RE_ALNUM_UNDERSCORE = re.compile(r'[^a-zA-Z0-9_]')
_RE_ALNUM_SPACE_UNDERSCORE_DASH = re.compile(r'[^a-zA-Z0-9_\s-]')


def leave_only_letters_numbers_or_underscore(text):
    """Removes characters that are not letters, numbers, or an underscore from a string.

    Parameters
    ----------
    text : str
        the input string

    Returns
    -------
    str
        a new string containing only letters, numbers, and underscores
    """
    return _RE_ALNUM_UNDERSCORE.sub('', text)


def leave_letters_numbers_spaces_underscores_dashes(text):
    """Removes characters not in letters, numbers, space, underscore, or dash from a string.

    Parameters
    ----------
    text : str
        the input string

    Returns
    -------
    str
        a new string containing only letters, numbers, spaces, underscores, and dashes
    """
    return _RE_ALNUM_SPACE_UNDERSCORE_DASH.sub('', text)



if __name__ == "__main__":
    pass
