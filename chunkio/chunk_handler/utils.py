from typing import List


def check_mode(mode: str, accepted: str) -> bool:
    _mode_set = set(list(mode))
    _accepted_mode_set = set(list(accepted))
    if not _mode_set.difference(accepted):
        return True
    raise AssertionError(f"Provided mode '{mode}' does conflict with accepted modes: {accepted}")


def parse_lines(string: str, delimiter: str = "\n") -> (List[str], str):
    """
    Parses string for complete lines and the last incomplete line

    :param string: input text to parse
    :param delimiter: delimiter that indicates a line break, default '\\n'
    :return: list of complete lines, incomplete next line
    """

    _lines = string.split(delimiter)
    return _lines[:-1], _lines[-1]
