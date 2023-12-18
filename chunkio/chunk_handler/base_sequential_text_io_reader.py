import _io
import typing
import warnings

from types import TracebackType
from typing import BinaryIO, TextIO, Type, Iterator, AnyStr, Iterable, Optional, Any, Generator

from chunkio.chunk_handler.format import BaseChunkFormat, SubdirNumberedChunkFormat


class BaseSequentialTextIOReader(typing.TextIO):
    def __init__(
            self,
            file_path: str,
            *open_args,
            mode: str = "r",
            chunk_format: BaseChunkFormat = SubdirNumberedChunkFormat(),
            verbose: bool = True,
            **open_kwargs
    ):
        self.file_path = file_path
        self.open_args = open_args
        self._mode = mode
        self.chunk_format = chunk_format
        self.verbose = verbose
        self.open_kwargs = open_kwargs

        self._current_file = None
        self._file_generator = self._set_file_generator()

    def _set_file_generator(self) -> Generator[TextIO, None, None]:
        for file_path in self.chunk_format.walk(self.file_path):
            yield open(file_path, mode=self.mode, *self.open_args, **self.open_kwargs)

    def _open_next_file(self):
        _old_file = self._current_file  # Remember old file

        self._current_file = next(self._file_generator)

        # Close file only after opening a new file successfully!
        if isinstance(_old_file, _io.TextIOWrapper):
            _old_file.close()

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def name(self) -> str:
        return self.file_path

    @property
    def closed(self) -> bool:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.closed
        else:
            return True

    @property
    def buffer(self) -> BinaryIO:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.buffer
        else:
            raise NotImplementedError("Buffer not initialized yet!")

    @property
    def encoding(self) -> str:
        _encoding = self.open_kwargs.get("encoding")
        if isinstance(_encoding, str):
            return _encoding
        elif isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.encoding
        else:
            raise NotImplementedError("Encoding not initialized yet!")

    @property
    def errors(self) -> Optional[str]:
        _errors = self.open_kwargs.get("errors")
        if isinstance(_errors, str):
            return _errors
        elif isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.errors
        else:
            return None

    @property
    def line_buffering(self) -> bool | int:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.line_buffering
        else:
            return False

    @property
    def newlines(self) -> Any:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.newlines
        else:
            raise NotImplementedError("Newlines not initialized yet!")

    def __enter__(self) -> TextIO:
        return self

    def close(self) -> None:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.close()
        else:
            return None

    def fileno(self) -> int:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.fileno()
        else:
            raise NotImplementedError("Current file not initialized yet!")

    def flush(self) -> None:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.flush()
        else:
            return None

    def isatty(self) -> bool:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.isatty()
        else:
            return False

    def read(self, __n: int = -1, *args) -> AnyStr:
        # ToDo: This might be possible to implement properly, at a later time. Currently not the intended use-case.
        if self.verbose:
            warnings.warn("read forwards to current chunk file. This may not be what you have intended to do! "
                          "(To silence this warning set verbose to false)")
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.read(*args)
        else:
            raise NotImplementedError("Current file not initialized yet!")

    def readable(self) -> bool:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.readable()
        else:
            return False

    def readline(self, __limit: int = -1, *args) -> AnyStr:
        if self.closed:
            try:
                self._open_next_file()
            except StopIteration:
                return ""

        while True:
            line = self._current_file.readline(__limit, *args)
            if line == "":
                try:
                    self._open_next_file()
                except StopIteration:
                    return ""
            else:
                return line

    def readlines(self, __hint: int = -1, *args) -> list[AnyStr]:
        if __hint <= 0:
            return [line for line in self]

        lines = []
        _current_hint = __hint

        if self.closed:
            try:
                self._open_next_file()
            except StopIteration:
                return lines

        while True:
            _new_lines = self._current_file.readlines(_current_hint, *args)
            lines.extend(_new_lines)
            _current_hint -= sum([len(line) for line in _new_lines])
            if _current_hint > 0:
                try:
                    self._open_next_file()
                except StopIteration:
                    break
            else:
                break

        return lines

    def seek(self, *args, **kwargs) -> int:
        # ToDo: This might be possible to implement properly, at a later time. Currently not the intended use-case.
        if self.verbose:
            warnings.warn("seek forwards to current chunk file. This may not be what you have intended to do! "
                          "(To silence this warning set verbose to false)")
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.seek(*args, **kwargs)
        else:
            raise NotImplementedError("Current file not initialized yet!")

    def seekable(self) -> bool:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.seekable()
        else:
            return False

    def tell(self) -> int:
        # ToDo: This might be possible to implement properly, at a later time. Currently not the intended use-case.
        if self.verbose:
            warnings.warn("tell forwards to current chunk file. This may not be what you have intended to do! "
                          "(To silence this warning set verbose to false)")
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.tell()
        else:
            raise NotImplementedError("Current file not initialized yet!")

    def truncate(self, *args, **kwargs) -> int:
        # ToDo: This might be possible to implement properly, at a later time. Currently not the intended use-case.
        if self.verbose:
            warnings.warn("truncate forwards to current chunk file. This may not be what you have intended to do! "
                          "(To silence this warning set verbose to false)")
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.truncate(*args, **kwargs)
        else:
            raise NotImplementedError("Current file not initialized yet!")

    def writable(self) -> bool:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.writable()
        else:
            return False

    def write(self, __s: AnyStr) -> int:
        raise NotImplementedError

    def writelines(self, __lines: Iterable[AnyStr]) -> None:
        raise NotImplementedError

    def __next__(self) -> AnyStr:
        if self.closed:
            self._open_next_file()
        while True:
            try:
                return next(self._current_file)
            except StopIteration:
                self._open_next_file()

    def __iter__(self) -> Iterator[AnyStr]:
        return self

    def __exit__(self, __t: Type[BaseException] | None, __value: BaseException | None,
                 __traceback: TracebackType | None) -> None:
        if isinstance(self._current_file, _io.TextIOWrapper):
            self._current_file.close()
