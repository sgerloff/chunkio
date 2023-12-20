import pytest

from typing import List

from chunkio.chunk_handler.utils import parse_lines


@pytest.mark.parametrize(
    "input_string, expected_lines, delimiter", [
        ("line\nline\n", ["line", "line"], "\n"),
        ("line\nline", ["line", "line"], "\n"),
        ("\nline\n\nline\n\n", ["", "line", "", "line", ""], "\n")
    ]
)
def test_parse_lines(input_string: str, expected_lines: List[str], delimiter: str):
    assert parse_lines(input_string, delimiter=delimiter) == expected_lines
