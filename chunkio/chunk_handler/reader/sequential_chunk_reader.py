from chunkio.chunk_handler.format import BaseChunkFormat, SubdirNumberedChunkFormat


class SequentialChunkReader:
    def __init__(
            self,
            file_path: str,
            mode: str,
            *open_args,
            chunk_format: BaseChunkFormat = SubdirNumberedChunkFormat(index_format="06d"),
            **open_kwargs
    ):

        self.file_path = file_path
        self.mode = mode
        self.chunk_format = chunk_format

        self.open_args = open_args
        self.open_kwargs = open_kwargs

        assert self.mode in ["r", "rt", "tr"], f"{self.__class__.__name__} is read only! (Supported modes: 'r', 'rt')"
        self._current_file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._current_file is not None:
            return self._current_file.__exit__(exc_type, exc_val, exc_tb)

    def __iter__(self):
        for file_path in self.chunk_format.walk(self.file_path):
            self._current_file = open(file_path, self.mode, *self.open_args, **self.open_kwargs)
            for line in self._current_file:
                yield line
            self._current_file.close()
