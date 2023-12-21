import _io
import typing
import warnings

from types import TracebackType
from typing import BinaryIO, TextIO, Type, Iterator, AnyStr, Iterable, Optional, Any, List

from chunkio.chunk_handler.format import BaseChunkFormat, SubdirNumberedChunkFormat
from chunkio.chunk_handler.chunker import SequentialChunker, MaxLineSequentialChunker
from .utils import check_mode, parse_lines


class BaseSequentialTextIOWriter(typing.TextIO):
    def __init__(
            self,
            file_path: str,
            *open_args,
            mode: str = "w",
            chunk_format: BaseChunkFormat = SubdirNumberedChunkFormat(),
            chunker: SequentialChunker = MaxLineSequentialChunker(max_lines=1_000),
            delimiter: str = "\n",
            verbose: bool = True,
            **open_kwargs
    ):
        check_mode(mode, "wt")

        self.file_path = file_path
        self._mode = mode

        self.chunk_format = chunk_format
        self.chunker = chunker
        self._delimiter = delimiter

        self.verbose = verbose

        self.open_args = open_args
        self.open_kwargs = open_kwargs

        self._current_file = None
        self._current_line = ""
        self._current_chunk_index = None
        self._open_chunk_file(self.chunker.current_index)

    def _open_chunk_file(self, index: int):
        if isinstance(self._current_file, _io.TextIOWrapper):
            self._current_file.close()
        self._current_chunk_index = index
        self._current_file = open(
            self.chunk_format.format(self.file_path, index),
            mode=self.mode,
            *self.open_args, **self.open_kwargs
        )

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
        raise NotImplementedError

    def readable(self) -> bool:
        if isinstance(self._current_file, _io.TextIOWrapper):
            return self._current_file.readable()
        else:
            return False

    def readline(self, __limit: int = -1, *args) -> AnyStr:
        raise NotImplementedError

    def readlines(self, __hint: int = -1, *args) -> list[AnyStr]:
        raise NotImplementedError

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
        lines = parse_lines(__s, delimiter=self._delimiter)
        return self._write_delimited_lines(lines)

    def writelines(self, __lines: Iterable[AnyStr]) -> None:
        _ = self._write_delimited_lines(list(__lines))
        return None

    def _write_delimited_lines(self, delimited_lines: List[str]) -> int:
        last_response = -1
        indices = self.chunker.indices(delimited_lines)
        _current_line = ""
        for line, index in zip(delimited_lines, indices):
            if index == self._current_chunk_index:
                _current_line += line
            else:
                last_response = self._current_file.write(_current_line)
                self._open_chunk_file(index)
                _current_line = line
        if _current_line:
            last_response = self._current_file.write(_current_line)
        return last_response

    def __next__(self) -> AnyStr:
        raise NotImplementedError

    def __iter__(self) -> Iterator[AnyStr]:
        return self

    def __exit__(self, __t: Type[BaseException] | None, __value: BaseException | None,
                 __traceback: TracebackType | None) -> None:
        if isinstance(self._current_file, _io.TextIOWrapper):
            self._current_file.close()
