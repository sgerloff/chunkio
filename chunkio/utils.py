import os


def get_chunk_file_path(file_path: str, index: int, index_format: str = "06d") -> str:
    """
    Builds file paths for a chunk file with provided index.

    :param file_path: base file path, becoming a directory to hold the file chunks
    :param index: index of the particular file chunk
    :param index_format: fstring format for the index in the file chunk path
    :return: path to file chunk
    """

    assert index >= 0, "Index should be an unsigned positive integer!"

    file_name = os.path.basename(file_path)
    file_name, file_ext = os.path.splitext(file_name)
    return os.path.join(file_path, f"{file_name}.{index:{index_format}}{file_ext}")
