import builtins

from typing import Optional
from io import IOBase


def open(*args, num_chunks: Optional[int] = None, **kwargs):
    return builtins.open(*args, **kwargs)
