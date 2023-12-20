from typing import List


def check_mode(mode: str, accepted: str) -> bool:
    _mode_set = set(list(mode))
    _accepted_mode_set = set(list(accepted))
    if not _mode_set.difference(accepted):
        return True
    raise AssertionError(f"Provided mode '{mode}' does conflict with accepted modes: {accepted}")


def parse_lines(string: str, delimiter: str = "\n") -> List[str]:
    if string.endswith(delimiter):
        return string[:-len(delimiter)].split(delimiter)
    else:
        return string.split(delimiter)
