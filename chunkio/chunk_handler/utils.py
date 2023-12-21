from typing import List


def check_mode(mode: str, accepted: str) -> bool:
    _mode_set = set(list(mode))
    _accepted_mode_set = set(list(accepted))
    if not _mode_set.difference(accepted):
        return True
    raise AssertionError(f"Provided mode '{mode}' does conflict with accepted modes: {accepted}")


def parse_lines(string: str, delimiter: str = "\n") -> List[str]:
    """
    Parses string for complete lines and the last incomplete line

    :param string: input text to parse
    :param delimiter: delimiter that indicates a line break, default '\\n'
    :return: list of complete lines, incomplete next line
    """
    _splits = string.split(delimiter)
    _split_sep = [line+delimiter for line in _splits]
    _split_sep[-1] = _splits[-1]
    return _split_sep


def group_into_lines(strings: List[str], delimiter: str = "\n") -> List[List[str]]:
    """
    Groups writes into lines according to provided delimiter. E.g. ['fir', 'st\n', 'l', 'i', 'n', 'e\n'] should yield
    [['fir', 'st\n'], ['l', 'i', 'n', 'e\n']]

    :param strings: list of strings to group
    :param delimiter: string that indicates a line break
    :return: list of grouped strings
    """
    _grouped_lines = [[]]
    for string in strings:
        _grouped_lines[-1].append(string)
        if string.endswith(delimiter):
            _grouped_lines.append([])
    if len(_grouped_lines[-1]) == 0:
        return _grouped_lines[:-1]
    else:
        return _grouped_lines
