import os.path
import shutil

import pytest
import chunkio


def test_write_num_chunks_end_to_end():
    EXPECTED_NUM_CHUNKS = 8
    NUM_LINES = 3
    NUM_TOTAL_LINES = NUM_LINES * EXPECTED_NUM_CHUNKS

    # Initialize file location
    TMP_DIR = "/tmp/chunkio"
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    BASE_FILE_NAME = os.path.join(TMP_DIR, "write_fixed_file_number.txt")
    os.makedirs(TMP_DIR, exist_ok=True)

    # Write file chunk
    input_data = list(range(NUM_TOTAL_LINES))
    with chunkio.open(BASE_FILE_NAME, "w", max_lines=NUM_LINES) as chunked_file:
        for i in input_data:
            chunked_file.write(f"{i}\n")

    # Chunks are written to a directory
    pytest.assume(os.path.isdir(BASE_FILE_NAME))
    pytest.assume(len(os.listdir(BASE_FILE_NAME)) == EXPECTED_NUM_CHUNKS)

    output_data = []
    with chunkio.open(BASE_FILE_NAME, "r") as chunked_file:
        for line in chunked_file:
            output_data.append(int(line))
    pytest.assume(input_data == output_data)

    # Clean up tmp dirs
    shutil.rmtree(TMP_DIR, ignore_errors=True)


def test_writelines_num_chunks_end_to_end():
    EXPECTED_NUM_CHUNKS = 8
    NUM_LINES = 3
    NUM_TOTAL_LINES = NUM_LINES * EXPECTED_NUM_CHUNKS

    # Initialize file location
    TMP_DIR = "/tmp/chunkio"
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    BASE_FILE_NAME = os.path.join(TMP_DIR, "write_fixed_file_number.txt")
    os.makedirs(TMP_DIR, exist_ok=True)

    # Write file chunk
    input_data = list(range(NUM_TOTAL_LINES))
    with chunkio.open(BASE_FILE_NAME, "w", max_lines=NUM_LINES) as chunked_file:
        chunked_file.writelines([f"{i}" for i in input_data])

    # Chunks are written to a directory
    pytest.assume(os.path.isdir(BASE_FILE_NAME))
    pytest.assume(len(os.listdir(BASE_FILE_NAME)) == EXPECTED_NUM_CHUNKS)

    output_data = []
    with chunkio.open(BASE_FILE_NAME, "r") as chunked_file:
        for line in chunked_file:
            output_data.append(int(line))
    pytest.assume(input_data == output_data)

    # Clean up tmp dirs
    shutil.rmtree(TMP_DIR, ignore_errors=True)
