import builtins

from typing import Optional

from chunkio.chunk_handler import MaxLineChunkWriter, BaseSequentialTextIOReader, SubdirNumberedChunkFormat, BaseSequentialTextIOWriter, MaxLineSequentialChunker


def open(file_path: str,
         mode: str = "r",
         max_lines: Optional[int] = None,
         keep_extension: bool = True,
         index_format: str = "06d",
         verbose: bool = True,
         *args, **kwargs):
    _delimiter = "\n"
    chunk_format = SubdirNumberedChunkFormat(index_format=index_format, keep_extension=keep_extension)
    if "r" in mode:
        return BaseSequentialTextIOReader(file_path,
                                          mode=mode,
                                          chunk_format=chunk_format,
                                          verbose=verbose,
                                          *args,
                                          **kwargs)
    elif "w" in mode and max_lines:
        chunker = MaxLineSequentialChunker(max_lines=max_lines, delimiter=_delimiter)
        return BaseSequentialTextIOWriter(file_path,
                                          mode=mode,
                                          chunk_format=chunk_format,
                                          chunker=chunker,
                                          verbose=verbose,
                                          delimiter=_delimiter,
                                          *args, **kwargs)
    else:
        return builtins.open(file_path, *args, **kwargs)
