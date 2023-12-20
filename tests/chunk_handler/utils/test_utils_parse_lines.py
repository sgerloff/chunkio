import pytest

from typing import List

from chunkio.chunk_handler.utils import parse_lines


@pytest.mark.parametrize(
    "delimiter, input_string, expected_lines, expected_incomplete_line", [
        ("\n", "line\nline\n", ["line", "line"], ""),
        ("\n", "line\nline", ["line"], "line"),
        ("\n", "\nline\n\nline\n\nline", ["", "line", "", "line", ""], "line"),
        ("<line break>",
         "<line break>line<line break><line break>line<line break><line break>line",
         ["", "line", "", "line", ""], "line")
    ]
)
def test_parse_lines(delimiter: str, input_string: str, expected_lines: List[str], expected_incomplete_line: str):
    assert parse_lines(input_string, delimiter=delimiter) == (expected_lines, expected_incomplete_line)
