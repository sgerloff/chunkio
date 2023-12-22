import builtins

from typing import Optional

from chunkio.chunk_handler import MaxLineChunkWriter, BaseSequentialTextIOReader, SubdirNumberedChunkFormat, BaseSequentialTextIOWriter, MaxLineSequentialChunker


def open(file_path: str, mode: str = "r", *args, max_lines: Optional[int] = None, **kwargs):
    chunk_format = SubdirNumberedChunkFormat()
    if "r" in mode:
        return BaseSequentialTextIOReader(file_path,
                                          mode=mode,
                                          chunk_format=chunk_format,
                                          *args,
                                          **kwargs)
    elif "w" in mode and max_lines:
        chunker = MaxLineSequentialChunker(max_lines=max_lines, delimiter="\n")
        return BaseSequentialTextIOWriter(file_path,
                                          mode=mode,
                                          chunk_format=chunk_format,
                                          chunker=chunker,
                                          *args, **kwargs)
    else:
        return builtins.open(file_path, *args, **kwargs)
