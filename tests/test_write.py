import os.path

import pytest
import chunkio

from chunkio.utils import get_chunk_file_path


def test_write_num_chunks_end_to_end():
    NUM_CHUNKS = 8
    NUM_LINES = 3
    NUM_TOTAL_LINES = NUM_LINES * NUM_CHUNKS

    # Initialize file location
    TMP_DIR = "/tmp/chunkio"
    BASE_FILE_NAME = os.path.join(TMP_DIR, "write_fixed_file_number.txt")
    os.makedirs(TMP_DIR, exist_ok=True)

    # Write file chunk
    input_data = list(range(NUM_TOTAL_LINES))
    with chunkio.open(BASE_FILE_NAME, "w", num_chunks=8) as chunked_file:
        for i in input_data:
            chunked_file.write(f"{i}\n")

    # Chunks are written to a directory
    pytest.assume(os.path.isdir(BASE_FILE_NAME))
    pytest.assume(len(os.listdir(BASE_FILE_NAME)) == NUM_CHUNKS)

    output_data = []
    with chunkio.open(BASE_FILE_NAME, "r") as chunked_file:
        for line in chunked_file:
            output_data.append(int(line))
    pytest.assume(input_data == output_data)

    # Clean up tmp dirs
    os.removedirs(TMP_DIR)


def test_writelines_num_chunks_end_to_end():
    NUM_CHUNKS = 8
    NUM_LINES = 3
    NUM_TOTAL_LINES = NUM_LINES * NUM_CHUNKS

    # Initialize file location
    TMP_DIR = "/tmp/chunkio"
    BASE_FILE_NAME = os.path.join(TMP_DIR, "write_fixed_file_number.txt")
    os.makedirs(TMP_DIR, exist_ok=True)

    # Write file chunk
    input_data = list(range(NUM_TOTAL_LINES))
    with chunkio.open(BASE_FILE_NAME, "w", num_chunks=8) as chunked_file:
        chunked_file.writelines([f"{i}" for i in input_data])

    # Chunks are written to a directory
    pytest.assume(os.path.isdir(BASE_FILE_NAME))
    pytest.assume(len(os.listdir(BASE_FILE_NAME)) == NUM_CHUNKS)

    output_data = []
    with chunkio.open(BASE_FILE_NAME, "r") as chunked_file:
        for line in chunked_file:
            output_data.append(int(line))
    pytest.assume(input_data == output_data)

    # Clean up tmp dirs
    os.removedirs(TMP_DIR)
