import pytest

from chunkio.chunk_handler import MaxLineSequentialChunker
from typing import List


@pytest.mark.parametrize(
    "max_lines, num_lines, expected_index", [
        (10, 1, 0),
        (10, 10, 0),
        (10, 11, 1),
        (10, 100, 9),
        (10, 101, 10)
    ]
)
def test_max_line_sequential_chunker_index(max_lines: int, num_lines: int, expected_index: int):
    chunker = MaxLineSequentialChunker(max_lines=max_lines)
    last_index = None
    for _ in range(num_lines):
        last_index = chunker.index("\n")
    assert last_index == expected_index


@pytest.mark.parametrize(
    "max_lines, previous_num_lines, num_lines, expected_indices", [
        (10, 0, 2, [0, 0]),
        (10, 9, 2, [0, 1]),
        (10, 109, 12, [10] + (10 * [11]) + [12])
    ]
)
def test_max_line_sequential_chunker_indices(max_lines: int,
                                             previous_num_lines: int,
                                             num_lines: int,
                                             expected_indices: List[int]):
    chunker = MaxLineSequentialChunker(max_lines=max_lines)

    # Prime the chunker to a specified line number
    _ = chunker.indices(["\n" for _ in range(previous_num_lines)])

    indices = chunker.indices(["\n" for _ in range(num_lines)])
    assert indices == expected_indices
