import re


def leave_only_letters_numbers_or_underscore(text):
    """Removes characters that are not letters, numbers, or an underscore from a string.

    Args:
      text: The input string.

    Returns:
      A new string containing only letter characters.
    """
    return re.sub(r'[^a-zA-Z0-9_]', '', text)


def leave_letters_numbers_spaces_underscores_dashes(text):
    """Removes characters not in letters, numbers, space, underscore, or dash from a string.

    Args:
      text: The input string.

    Returns:
      A new string containing only letter characters.
    """
    return re.sub(r'[^a-zA-Z0-9_\s-]', '', text)



if __name__ == "__main__":
    pass
