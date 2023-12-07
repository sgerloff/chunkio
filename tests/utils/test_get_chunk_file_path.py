import pytest

from chunkio.utils import get_chunk_file_path


@pytest.mark.parametrize(
    "input_file_path,index,expected_file_path", [
        ("/tmp/test_file.txt", 0, "/tmp/test_file.txt/test_file.000000.txt"),
        ("/tmp/test_file.txt", 1234567, "/tmp/test_file.txt/test_file.1234567.txt")
    ]
)
def test_get_chunk_file_path(input_file_path: str, index: int, expected_file_path: str):
    assert get_chunk_file_path(input_file_path, index) == expected_file_path


@pytest.mark.parametrize(
    "input_file_path,index,format_str", [
        ("/tmp/test_file.txt", -1, "06d")
    ]
)
def test_get_chunk_file_path_assertion_error(input_file_path: str, index: int, format_str: str):
    with pytest.raises(AssertionError):
        _ = get_chunk_file_path(input_file_path, index, index_format=format_str)
