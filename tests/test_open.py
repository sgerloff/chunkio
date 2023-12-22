import os
import pytest
import shutil
import chunkio
import builtins

from typing import List


TEST_DATA_FUll = [
    "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, "
    "sed diam nonumy eirmod tempor invidunt ut labore et dolore "
    "magna aliquyam erat, sed diam voluptua.\nAt vero eos et "
    "accusam et justo duo dolores et ea rebum.\nStet clita kasd "
    "gubergren, no sea takimata sanctus est Lorem ipsum dolor "
    "sit amet.\nLorem ipsum dolor sit amet, consetetur sadipscing "
    "elitr, sed diam nonumy eirmod tempor invidunt ut labore et "
    "dolore magna aliquyam erat, sed diam voluptua.\nAt vero eos "
    "et accusam et justo duo dolores et ea rebum.\nStet clita kasd "
    "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
    "amet."
]

TEST_DATA_FRAGMENTED_LINEBREAKS = [
    "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, "
    "sed diam nonumy eirmod tempor invidunt ut labore et dolore "
    "magna aliquyam erat, sed diam voluptua.\n",
    "At vero eos et accusam et justo duo dolores et ea rebum.\n",
    "Stet clita kasd gubergren, no sea takimata sanctus est "
    "Lorem ipsum dolor sit amet.\n",
    "Lorem ipsum dolor sit amet, consetetur sadipscing "
    "elitr, sed diam nonumy eirmod tempor invidunt ut labore et "
    "dolore magna aliquyam erat, sed diam voluptua.\n",
    "At vero eos et accusam et justo duo dolores et ea rebum.\n",
    "Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
    "amet."
]

TEST_DATA_FRAGMENTED_RANDOM = [
    "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, ",
    "sed diam nonumy eirmod tempor invidunt ut labore et dolore ",
    "magna aliquyam erat, sed diam voluptua.\nAt vero eos et ",
    "accusam et justo duo dolores et ea rebum.\nStet clita kasd ",
    "gubergren, no sea takimata sanctus est Lorem ipsum dolor ",
    "sit amet.\nLorem ipsum dolor sit amet, consetetur sadipscing ",
    "elitr, sed diam nonumy eirmod tempor invidunt ut labore et ",
    "dolore magna aliquyam erat, sed diam voluptua.\nAt vero eos ",
    "et accusam et justo duo dolores et ea rebum.\nStet clita kasd ",
    "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit ",
    "amet."
]


@pytest.mark.parametrize(
    "data, builtins_args, chunkio_args, builtins_kwargs, chunkio_kwargs", [
    (TEST_DATA_FUll, [], [], dict(), dict(max_lines=1)),
    (TEST_DATA_FRAGMENTED_LINEBREAKS, [], [], dict(), dict(max_lines=1)),
    (TEST_DATA_FRAGMENTED_RANDOM, [], [], dict(), dict(max_lines=1)),
    (TEST_DATA_FUll, [], [], dict(), dict(max_lines=2)),
    (TEST_DATA_FRAGMENTED_LINEBREAKS, [], [], dict(), dict(max_lines=2)),
    (TEST_DATA_FRAGMENTED_RANDOM, [], [], dict(), dict(max_lines=2)),
    (TEST_DATA_FUll, [], [], dict(), dict(max_lines=100_000)),
    (TEST_DATA_FRAGMENTED_LINEBREAKS, [], [], dict(), dict(max_lines=100_000)),
    (TEST_DATA_FRAGMENTED_RANDOM, [], [], dict(), dict(max_lines=100_000)),
])
def test_open_write(data: List[str],
                    builtins_args: list,
                    chunkio_args: list,
                    builtins_kwargs: dict,
                    chunkio_kwargs: dict):
    tmp_dir = "/tmp/chunkio"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir)
    chunkio_file_name = os.path.join(tmp_dir, "chunkio_open_write_test.txt")
    builtin_file_name = os.path.join(tmp_dir, "builtin_open_write_test.txt")

    # Write files
    with (chunkio.open(chunkio_file_name, mode="w", *chunkio_args, **chunkio_kwargs) as chunkio_file,
          builtins.open(builtin_file_name, mode="w", *builtins_args, **builtins_kwargs) as builtins_file):
        for text in data:
            chunkio_file.write(text)
            builtins_file.write(text)

    # Read files to check
    with (chunkio.open(chunkio_file_name, mode="r", *chunkio_args, **chunkio_kwargs) as chunkio_file,
          builtins.open(builtin_file_name, mode="r", *builtins_args, **builtins_kwargs) as builtins_file):
        chunkio_read_data = [line for line in chunkio_file]
        expected_read_data = [line for line in builtins_file]

    assert chunkio_read_data == expected_read_data
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.mark.parametrize(
    "data, builtins_args, chunkio_args, builtins_kwargs, chunkio_kwargs", [
    (TEST_DATA_FUll, [], [], dict(), dict(max_lines=1)),
    (TEST_DATA_FRAGMENTED_LINEBREAKS, [], [], dict(), dict(max_lines=1)),
    (TEST_DATA_FRAGMENTED_RANDOM, [], [], dict(), dict(max_lines=1)),
    (TEST_DATA_FUll, [], [], dict(), dict(max_lines=2)),
    (TEST_DATA_FRAGMENTED_LINEBREAKS, [], [], dict(), dict(max_lines=2)),
    (TEST_DATA_FRAGMENTED_RANDOM, [], [], dict(), dict(max_lines=2)),
    (TEST_DATA_FUll, [], [], dict(), dict(max_lines=100_000)),
    (TEST_DATA_FRAGMENTED_LINEBREAKS, [], [], dict(), dict(max_lines=100_000)),
    (TEST_DATA_FRAGMENTED_RANDOM, [], [], dict(), dict(max_lines=100_000)),
])
def test_open_writelines(data: List[str],
                    builtins_args: list,
                    chunkio_args: list,
                    builtins_kwargs: dict,
                    chunkio_kwargs: dict):
    tmp_dir = "/tmp/chunkio"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir)
    chunkio_file_name = os.path.join(tmp_dir, "chunkio_open_write_test.txt")
    builtin_file_name = os.path.join(tmp_dir, "builtin_open_write_test.txt")

    # Write files
    with (chunkio.open(chunkio_file_name, mode="w", *chunkio_args, **chunkio_kwargs) as chunkio_file,
          builtins.open(builtin_file_name, mode="w", *builtins_args, **builtins_kwargs) as builtins_file):
        chunkio_file.writelines(data)
        builtins_file.writelines(data)

    # Read files to check
    with (chunkio.open(chunkio_file_name, mode="r", *chunkio_args, **chunkio_kwargs) as chunkio_file,
          builtins.open(builtin_file_name, mode="r", *builtins_args, **builtins_kwargs) as builtins_file):
        chunkio_read_data = [line for line in chunkio_file]
        expected_read_data = [line for line in builtins_file]

    assert chunkio_read_data == expected_read_data
    shutil.rmtree(tmp_dir, ignore_errors=True)

