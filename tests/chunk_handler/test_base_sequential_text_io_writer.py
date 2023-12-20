import pytest
import os
import shutil

from typing import List, Dict

from chunkio.chunk_handler.chunker import MaxLineSequentialChunker
from chunkio.chunk_handler.base_sequential_text_io_writer import BaseSequentialTextIOReader


@pytest.mark.parametrize(
    "max_lines, base_file_path, writes, expected_data", [
        (
                2,
                "/tmp/chunkio/base_sequential_text_io_writer.txt",
                ["first\n", "sec", "ond\nt", "hi", "rd\n", "four", "th"],
                {
                    "base_sequential_text_io_writer.000000.txt": [
                        "first\n", "second\n"
                    ],
                    "base_sequential_text_io_writer.000001.txt": [
                        "third\n", "fourth"
                    ]
                }
        )
    ]
)
def test_base_sequential_text_io_writer_write(max_lines: int,
                                              base_file_path: str,
                                              writes: List[str],
                                              expected_data: Dict[str, str]):
    shutil.rmtree(base_file_path, ignore_errors=True)  # Reset
    _chunker = MaxLineSequentialChunker(max_lines=max_lines)
    with BaseSequentialTextIOReader(base_file_path, mode="w", chunker=_chunker) as file:
        for _write in writes:
            file.write(_write)
    for file_path, expected_lines in expected_data.items():
        with open(os.path.join(base_file_path, file_path), "r") as file:
            lines = [line for line in file]
        pytest.assume(lines == expected_lines)
    shutil.rmtree(base_file_path, ignore_errors=True)  # Clean up


@pytest.mark.parametrize(
    "max_lines, base_file_path, lines, expected_data", [
        (
                2,
                "/tmp/chunkio/base_sequential_text_io_writer.txt",
                ["first\n", "second\n", "third\n", "fourth"],
                {
                    "base_sequential_text_io_writer.000000.txt": [
                        "first\n", "second\n"
                    ],
                    "base_sequential_text_io_writer.000001.txt": [
                        "third\n", "fourth"
                    ]
                }
        )
    ]
)
def test_base_sequential_text_io_writer_writelines(max_lines: int,
                                                   base_file_path: str,
                                                   lines: List[str],
                                                   expected_data: Dict[str, str]):
    shutil.rmtree(base_file_path, ignore_errors=True)  # Reset
    _chunker = MaxLineSequentialChunker(max_lines=max_lines)
    with BaseSequentialTextIOReader(base_file_path, mode="w", chunker=_chunker) as file:
        file.writelines(lines)
    for file_path, expected_lines in expected_data.items():
        with open(os.path.join(base_file_path, file_path), "r") as file:
            _lines = [line for line in file]
        pytest.assume(_lines == expected_lines)
    shutil.rmtree(base_file_path, ignore_errors=True)  # Clean up


def test_base_sequential_text_io_writer_mixed_writes():
    base_file_path = "/tmp/chunkio/base_sequential_text_io_writer.txt"
    max_lines = 2
    shutil.rmtree(base_file_path, ignore_errors=True)  # Reset
    _chunker = MaxLineSequentialChunker(max_lines=max_lines)
    with BaseSequentialTextIOReader(base_file_path, mode="w", chunker=_chunker) as file:
        file.write("fir")  # Start a line but don't finish
        file.writelines(["st", "second"])
        file.write("third\nfourth")
    with open(os.path.join(base_file_path, "base_sequential_text_io_writer.000000.txt"), mode="r") as file:
        _lines = [line for line in file]
    pytest.assume(_lines == ["first\n", "second\n"])
    with open(os.path.join(base_file_path, "base_sequential_text_io_writer.000001.txt"), mode="r") as file:
        _lines = [line for line in file]
    pytest.assume(_lines == ["third\n", "fourth"])
    shutil.rmtree(base_file_path, ignore_errors=True)  # Clean up
