import pytest

from chunkio.chunk_handler import SubdirNumberedChunkFormat

@pytest.mark.parametrize(
    "file_path,index,expected", [
        ("/tmp/test_file.txt", 0, "/tmp/test_file.txt/test_file.000000.txt"),
        ("/tmp/test_file.txt", 1234567, "/tmp/test_file.txt/test_file.1234567.txt")
    ]
)
def test_format_subdir_numbered_chunk_format(file_path:str, index: int, expected: str):
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
