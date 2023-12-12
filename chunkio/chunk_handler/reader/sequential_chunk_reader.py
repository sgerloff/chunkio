import os
import contextlib


class SequentialChunkReader:
    def __init__(
            self,
            file_path: str,
            mode: str,
            *open_args,
            **open_kwargs
    ):
        self.file_path = file_path
        self.mode = mode
        self.open_args = open_args
        self.open_kwargs = open_kwargs

        assert "r" in self.mode, f"{self.__class__.__name__} is read only! (Supported modes: 'r', 'rb')"

        self._chunk_file_paths = sorted([os.path.join(file_path, file) for file in os.listdir(file_path)])
        self._current_file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._current_file is not None:
            return self._current_file.__exit__(exc_type, exc_val, exc_tb)

    def __iter__(self):
        for file_path in self._chunk_file_paths:
            self._current_file = open(file_path, self.mode, *self.open_args, **self.open_kwargs)
            for line in self._current_file:
                yield line
            self._current_file.close()
