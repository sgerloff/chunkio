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


def test_max_line_sequential_chunker_index_line_parts():
    max_lines = 10
    delimiter = "\n"
    chunker = MaxLineSequentialChunker(max_lines=max_lines, delimiter=delimiter)
    _ = [chunker.index("asdf" + delimiter) for _ in range(max_lines + 1)]
    _ = [chunker.index(" ") for _ in range(1234 * max_lines)]
    assert chunker.index("\n") == 1


@pytest.mark.parametrize(
    "max_lines, previous_num_lines, num_lines, expected_indices", [
        (10, 0, 11, (10 * [0]) + [1]),
        (10, 9, 2, [0, 1]),  # Span chunk
        (10, 10, 11, (10 * [1]) + [2]),  # Exhaust full chunk first
        (10, 109, 12, [10] + (10 * [11]) + [12])  # Span multiple chunks
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


@pytest.mark.parametrize(
    "delimiter, max_lines, prime_lines, line_parts, expected_indices", [
        (
                "\n", 2,
                3 * ["\n"],
                ["fir", "st\n", "s", "e", "c", "ond\n", "third\n", "four", "th"],
                [1, 1, 2, 2, 2, 2, 2, 3, 3]
        ), (
                "\n", 2,
                ["\n", "\n", "in", "complete"],
                ["fir", "st\n", "s", "e", "c", "ond\n", "third\n", "four", "th"],
                [1, 1, 1, 1, 1, 1, 2, 2, 2]
        )
    ]
)
def test_max_line_sequential_chunker_indices_line_parts(delimiter: str,
                                                        max_lines: int,
                                                        prime_lines: List[str],
                                                        line_parts: List[str],
                                                        expected_indices: List[int]):
    chunker = MaxLineSequentialChunker(max_lines=max_lines)
    _ = chunker.indices(prime_lines)
    assert chunker.indices(line_parts) == expected_indices
