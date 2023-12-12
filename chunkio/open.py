from typing import Optional

from chunk_handler import ChunkHandler


def open(*args, max_lines: Optional[int] = None, **kwargs):
    return ChunkHandler(*args, max_lines=max_lines, **kwargs)
