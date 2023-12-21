import pytest

from typing import List

from chunkio.chunk_handler.utils import parse_lines


@pytest.mark.parametrize(
    "delimiter, input_string, expected_lines", [
        ("\n", "line", ["line"]),
        ("\n", "line\nline\n", ["line\n", "line\n", ""]),
        ("\n", "line\nline", ["line\n", "line"]),
        ("\n", "\nline\n\nline\n\nline", ["\n", "line\n", "\n", "line\n", "\n", "line"]),
        (
                "<line break>",
                "<line break>line<line break><line break>line<line break><line break>line",
                ["<line break>", "line<line break>", "<line break>", "line<line break>", "<line break>", "line"]
        )
    ]
)
def test_parse_lines(delimiter: str, input_string: str, expected_lines: List[str]):
    assert parse_lines(input_string, delimiter=delimiter) == expected_lines
