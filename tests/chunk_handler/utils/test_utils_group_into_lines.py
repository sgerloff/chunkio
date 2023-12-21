import pytest

from typing import List

from chunkio.chunk_handler.utils import group_into_lines


@pytest.mark.parametrize(
    "delimiter, strings, expected_groups", [
        ("\n", ["fir", "st\n", "l", "i", "n", "e\n"], [["fir", "st\n"], ["l", "i", "n", "e\n"]]),
        ("\n", ["fir", "st\n", "l", "i", "n", "e"], [["fir", "st\n"], ["l", "i", "n", "e"]]),
    ]
)
def test_group_into_lines(delimiter: str, strings: List[str], expected_groups: List[List[str]]):
    assert group_into_lines(strings, delimiter=delimiter) == expected_groups