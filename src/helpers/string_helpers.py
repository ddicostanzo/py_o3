import re


def clean_table_and_column_names(text):
    """Removes all non-letter characters from a string.

    Args:
      text: The input string.

    Returns:
      A new string containing only letter characters.
    """
    return re.sub(r'[^a-zA-Z0-9_]', '', text)


def clean_text_values(text):
    """Removes all non-letter characters from a string.

    Args:
      text: The input string.

    Returns:
      A new string containing only letter characters.
    """
    return re.sub(r'[^a-zA-Z0-9_\s-]', '', text)


def replace_dash_with_to(text):
    """
    Replaces any dash in the text with the word To for column creation
    Args:
        text: input string.

    Returns:
      A new string with the dash replaced with the word To.
    """
    return text.replace('-', 'To')


if __name__ == "__main__":
    pass
