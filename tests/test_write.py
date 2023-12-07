import os.path

import pytest
import chunkio

from chunkio.utils import get_chunk_file_path


def test_write_num_chunks():
    NUM_CHUNKS = 8
    NUM_LINES = 3
    NUM_TOTAL_LINES = NUM_LINES * NUM_CHUNKS

    BASE_FILE_NAME = "/tmp/chunkio/write_fixed_file_number.txt"

    # Write File Chunk
    with chunkio.open(BASE_FILE_NAME, num_chunks=8) as chunked_file:
        for i in range(NUM_TOTAL_LINES):
            chunked_file.write(f"{i}\n")

    # Chunks are written to a directory
    pytest.assume(os.path.isdir(BASE_FILE_NAME))

    for i in range(NUM_CHUNKS):
        chunk_file_path = get_chunk_file_path(BASE_FILE_NAME, i)
        pytest.assume(os.path.isfile(chunk_file_path))

        with open(chunk_file_path, "r") as file:
            for line_number, line in enumerate(file):
                pytest.assume( i + line_number * NUM_CHUNKS == int(line) )
