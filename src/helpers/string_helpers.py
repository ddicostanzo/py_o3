import re


def strip_non_letters(text):
    """Removes all non-letter characters from a string.

    Args:
      text: The input string.

    Returns:
      A new string containing only letter characters.
    """
    return re.sub(r'[^a-zA-Z]', '', text)


if __name__ == "__main__":
  pass
