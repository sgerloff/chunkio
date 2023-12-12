from typing import Optional

from utils import get_chunk_file_path


class ChunkHandler:
    def __init__(
            self,
            file_path: str,
            *open_args,
            max_lines: Optional[int] = None,
            **open_kwargs
    ):
        self.file_path = file_path

        self.open_args = open_args
        self.open_kwargs = open_kwargs

        self.max_lines = max_lines
        self._current_file = None
        self._current_file_id = None
        self._current_file_count = None

    def __enter__(self):
        os.makedirs(self.file_path, exist_ok=True)
        self._get_current_file()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._current_file.__exit__(exc_type, exc_val, exc_tb)

    def write(self, line: str) -> int:
        _current_file = self._get_current_file()
        _write_result = _current_file.write(line)
        self._current_file_count += 1
        return _write_result

    def _get_current_file(self):
        if self._current_file is None:
            self._current_file_id = 0
            self._current_file_count = 0
            self._open_next_current_file()

        if self._current_file_count >= self.max_lines:
            self._current_file_id += 1
            self._current_file_count = 0
            self._open_next_current_file()

        return self._current_file

    def _open_next_current_file(self):
        _current_file_path = get_chunk_file_path(self.file_path, self._current_file_id)
        self._current_file = open(_current_file_path,
                                  *self.open_args,
                                  **self.open_kwargs)

    def __getattr__(self, item):
        """
        Methods not overwritten by the ChunkHandler should be forwarded to the current file.

        :param item: attribute name
        :return:
        """
        return self._current_file.__getattribute__(item)


if __name__ == "__main__":
    import os

    os.makedirs("/tmp/chunkio", exist_ok=True)
    with ChunkHandler("/tmp/chunkio/test.txt", "w", max_lines=3) as file:
        for i in range(30):
            file.write(f"{i}\n")
