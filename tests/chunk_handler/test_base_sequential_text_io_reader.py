import pytest
import os

from typing import List

from tests.utils import FileSystemBuilder
from chunkio.chunk_handler import BaseSequentialTextIOReader, SubdirNumberedChunkFormat

TEXT_WITH_EMPTY_FILES = "".join([
    "<test.txt/readlines.000000.txt>",
    "",  # Start empty file
    "<test.txt/readlines.000001.txt>",
    "first line\nsecond line\n",
    "<test.txt/readlines.000002.txt>",
    "",  # Consequtive empty file
    "<test.txt/readlines.000003.txt>",
    "",  # Consequitive empty file
    "<test.txt/readlines.000004.txt>",
    "third line\nforth line\n",
    "<test.txt/readlines.000005.txt>",
    ""  # End empty file
])


@pytest.mark.parametrize(
    "data, sizehint, expected_lines", [
        # Read all
        (TEXT_WITH_EMPTY_FILES, -1, ["first line\n", "second line\n", "third line\n", "forth line\n"]),
        (TEXT_WITH_EMPTY_FILES, 10, ["first line\n"]),
        (TEXT_WITH_EMPTY_FILES, 25, ["first line\n", "second line\n", "third line\n"])
    ]
)
def test_base_sequential_text_io_reader_readlines(data: str, sizehint: int, expected_lines: List[str]):
    _base_dir = "/tmp/chunkio"
    _base_file_path = os.path.join(_base_dir, "test.txt")
    with FileSystemBuilder(data, base_path=_base_dir, keep_files=False) as _:
        with BaseSequentialTextIOReader(_base_file_path, mode="r") as file:
            lines = file.readlines(sizehint)
        assert lines == expected_lines


@pytest.mark.parametrize(
    "data, size, expected_ten_lines", [
        (TEXT_WITH_EMPTY_FILES, -1, 
         ["first line\n", "second line\n", "third line\n", "forth line\n"] + 6*[""]),
        (TEXT_WITH_EMPTY_FILES, 9, 
         ['first lin', 'e\n', 'second li', 'ne\n', 'third lin', 'e\n', 'forth lin', 'e\n', '', ''])
    ]
)
def test_base_sequential_text_io_reader_readline(data: str, size: int, expected_ten_lines: List[str]):
    _base_dir = "/tmp/chunkio"
    _base_file_path = os.path.join(_base_dir, "test.txt")
    with FileSystemBuilder(data, base_path=_base_dir, keep_files=False) as _:
        with BaseSequentialTextIOReader(_base_file_path, mode="r") as file:
            ten_lines = [file.readline(size) for _ in range(10)]
        assert ten_lines == expected_ten_lines


@pytest.mark.parametrize(
    "data, expected_lines", [
        (TEXT_WITH_EMPTY_FILES, ["first line\n", "second line\n", "third line\n", "forth line\n"])
    ]
)
def test_base_sequential_text_io_reader_iter(data: str, expected_lines: List[str]):
    _base_dir = "/tmp/chunkio"
    _base_file_path = os.path.join(_base_dir, "test.txt")
    with FileSystemBuilder(data, base_path=_base_dir, keep_files=False) as _:
        with BaseSequentialTextIOReader(_base_file_path, mode="r") as file:
            ten_lines = [line for line in file]
        assert ten_lines == expected_lines


@pytest.mark.parametrize(
    "data, expected_lines", [
        (TEXT_WITH_EMPTY_FILES, ["firs", "t line\n", "second line\n", "third line\n", "forth ", "line\n", ""])
    ]
)
def test_base_sequential_text_io_reader_mixed_reads(data: str, expected_lines: List[str]):
    _base_dir = "/tmp/chunkio"
    _base_file_path = os.path.join(_base_dir, "test.txt")
    with FileSystemBuilder(data, base_path=_base_dir, keep_files=False) as _:
        lines = []
        with BaseSequentialTextIOReader(_base_file_path, mode="r") as file:
            # Partially read first line
            lines.append(file.readline(4))
            # Read some more lines up to sizehint 21
            lines.extend(file.readlines(21))
            # Read again a line partially
            lines.append(file.readline(6))
            # Read the rest from full iteration
            lines.extend([line for line in file])
            # Check response after exhausting content
            lines.append(file.readline())
            # Check that further iteration does not return more content
            lines.extend(file.readlines())
            lines.extend([line for line in file])
    assert lines == expected_lines
