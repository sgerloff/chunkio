import builtins

from typing import Optional

from chunkio.chunk_handler import MaxLineChunkWriter, SequentialChunkReader


def open(file_path: str, mode: str = "r", *args, max_lines: Optional[int] = None, **kwargs):
    if "r" in mode:
        return SequentialChunkReader(file_path, mode, *args, **kwargs)
    elif max_lines is not None:
        return MaxLineChunkWriter(file_path, mode, *args, max_lines=max_lines, **kwargs)
    else:
        return builtins.open(file_path, *args, **kwargs)
