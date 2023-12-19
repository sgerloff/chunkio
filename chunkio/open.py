import builtins

from typing import Optional

from chunkio.chunk_handler import MaxLineChunkWriter, BaseSequentialTextIOReader, SubdirNumberedChunkFormat


def open(file_path: str, mode: str = "r", *args, max_lines: Optional[int] = None, **kwargs):
    chunk_format = SubdirNumberedChunkFormat()
    if "r" in mode:
        return BaseSequentialTextIOReader(file_path,
                                          mode=mode,
                                          chunk_format=chunk_format,
                                          *args,
                                          **kwargs)
    elif max_lines is not None:
        return MaxLineChunkWriter(file_path, mode, *args, max_lines=max_lines, **kwargs)
    else:
        return builtins.open(file_path, *args, **kwargs)
