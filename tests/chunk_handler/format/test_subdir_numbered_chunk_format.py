import pytest
import shutil
import os

from chunkio.chunk_handler import SubdirNumberedChunkFormat


@pytest.mark.parametrize(
    "file_path,index,expected", [
        ("/tmp/test_file.txt", 0, "/tmp/test_file.txt/test_file.000000.txt"),
        ("/tmp/test_file.txt", 1234567, "/tmp/test_file.txt/test_file.1234567.txt")
    ]
)
def test_format_subdir_numbered_chunk_format(file_path: str, index: int, expected: str):
    chunk_format = SubdirNumberedChunkFormat(index_format="06d")
    assert chunk_format.format(file_path, index) == expected


@pytest.mark.parametrize(
    "file_path,index", [
        ("/tmp/test_file.txt", -1),
        ("/tmp/test_file.txt", 1.1),
        ("/tmp/test_file.txt", "one")
    ]
)
def test_format_subdir_numbered_chunk_format_assertion(file_path: str, index: int):
    chunk_format = SubdirNumberedChunkFormat(index_format="06d")
    with pytest.raises(AssertionError):
        _ = chunk_format.format(file_path, index)


@pytest.mark.parametrize(
    "file_path,expected_base_path,expected_index", [
        ("/tmp/test_file.txt/test_file.000000.txt", "/tmp/test_file.txt", 0),
        ("/tmp/test_file.txt/test_file.1234567.txt", "/tmp/test_file.txt", 1234567),
        ("/tmp/test_file.something.txt/test_file.something.001234.txt", "/tmp/test_file.something.txt", 1234),
    ]
)
def test_parse_subdir_numbered_chunk_format(file_path: str, expected_base_path: str, expected_index: int):
    chunk_format = SubdirNumberedChunkFormat(index_format="06d")
    assert chunk_format.parse(file_path) == (expected_base_path, expected_index)


@pytest.mark.parametrize(
    "file_path", [
        "/tmp/test_file.txt/test_file.one.txt",
        "/tmp/test_file.txt/test_file.-1.txt",
        "/tmp/test_file.txt/test_file.txt"
    ]
)
def test_parse_subdir_numbered_chunk_format_assertion(file_path: str):
    chunk_format = SubdirNumberedChunkFormat(index_format="06d")
    with pytest.raises(AssertionError):
        chunk_format.parse(file_path)


def test_walk_subdir_numbered_chunk_format():
    base_directory = "/tmp/chunkio/subdir_numbered_chunk_format.txt"
    shutil.rmtree(base_directory, ignore_errors=True)

    ordered_valid_files = [
        "subdir_numbered_chunk_format.000000.txt",
        "subdir_numbered_chunk_format.000001.txt",
        "subdir_numbered_chunk_format.123456.txt"
    ]
    invalid_dirs = [
        "subdir_numbered_chunk_format.000002.txt"
    ]
    invalid_files = [
        "subdir_numbered_chunk_format.txt",
        "subdir_numbered_chunk_format.two.txt",
        "subdir_numbered_chunk_format.-1.txt"
    ]

    os.makedirs(base_directory, exist_ok=True)
    for file_name in ordered_valid_files + invalid_files:
        with open(os.path.join(base_directory, file_name), "w") as file:
            file.write("\n")
    for dir_name in invalid_dirs:
        os.makedirs(os.path.join(base_directory, dir_name), exist_ok=True)

    chunk_format = SubdirNumberedChunkFormat(index_format="06d")
    expected_file_paths = [os.path.join(base_directory, file_name) for file_name in ordered_valid_files]

    assert list(chunk_format.walk(base_directory)) == expected_file_paths
