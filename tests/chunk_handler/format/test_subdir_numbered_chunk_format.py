import pytest

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
        ("/tmp/test_file.txt", "eins")
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
        "/tmp/test_file.txt/test_file.eins.txt",
        "/tmp/test_file.txt/test_file.-1.txt",
        "/tmp/test_file.txt/test_file.txt"
    ]
)
def test_parse_subdir_numbered_chunk_format_assertion(file_path: str):
    chunk_format = SubdirNumberedChunkFormat(index_format="06d")
    with pytest.raises(AssertionError):
        chunk_format.parse(file_path)