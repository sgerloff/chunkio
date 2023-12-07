import os


def get_chunk_file_path(file_path: str, index: int, index_format: str = "06d") -> str:
    assert index >= 0, "Index should be an unsigned positive integer!"
    file_name = os.path.basename(file_path)
    file_name, file_ext = os.path.splitext(file_name)
    return os.path.join(file_path, f"{file_name}.{index:{index_format}}{file_ext}")
