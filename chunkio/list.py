from chunkio.chunk_handler import SubdirNumberedChunkFormat


def list_chunks(file_path: str,
                keep_extension: bool = True,
                index_format: str = "06d",
                return_index: bool = False,
                *_, **__):
    chunk_format = SubdirNumberedChunkFormat(index_format=index_format, keep_extension=keep_extension)
    return chunk_format.list(file_path, return_index=return_index)
