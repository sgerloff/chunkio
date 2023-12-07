import builtins

from typing import Optional


def open(*args, num_chunks: Optional[int] = None, **kwargs):
    return builtins.open(*args, **kwargs)
