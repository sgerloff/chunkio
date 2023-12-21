from abc import ABC, abstractmethod
from typing import List, Optional

from chunkio.chunk_handler.utils import group_into_lines


class SequentialChunker(ABC):
    def __init__(self):
        self._current_index = None

    @property
    def current_index(self) -> Optional[int]:
        return self._current_index

    @abstractmethod
    def reset(self):
        """
        Reset chunker, e.g. current line counts, estimated file sizes, ...

        :return:
        """
        pass

    @abstractmethod
    def index(self, line: str) -> int:
        """
        Returns true if this line should be writen to the chunker chunk file, else returns false.

        :param line: line to be written
        :return: chunk index of the provided line
        """
        pass

    def indices(self, lines: List[str]) -> List[int]:
        """
        Split list of lines into chunks of lines written to the next chunk files.

        :param lines: list of lines (each being a string)
        :return: list of chunk indices for the provided list of lines
        """
        return [self.index(line) for line in lines]


class MaxLineSequentialChunker(SequentialChunker):
    def __init__(self, max_lines: int = 10_000, delimiter: str = "\n"):
        super().__init__()
        self.max_lines = max_lines
        self.delimiter = delimiter
        self.current_line_count = None
        self.reset()

    def reset(self):
        self.current_line_count = 0
        self._current_index = 0

    def index(self, line_part: str) -> int:
        if self.current_line_count >= self.max_lines:
            self.current_line_count = 0  # Reset line count
            self._current_index += 1

        if line_part.endswith(self.delimiter):
            self.current_line_count += 1
        return self._current_index

    def indices(self, line_parts: List[str]) -> List[int]:
        _remaining_lines = self.max_lines - self.current_line_count

        grouped_lines = group_into_lines(line_parts, delimiter=self.delimiter)
        grouped_lines_length = [len(group) for group in grouped_lines]
        lines_length = len(grouped_lines_length)

        indices = []
        if _remaining_lines > lines_length:  # Simple edge case that provided line parts fit the current file
            self.current_line_count += lines_length
            return len(line_parts) * [self._current_index]

        # First finish the incomplete current file
        line_parts_length = sum(
            [length for length in grouped_lines_length[:_remaining_lines]]
        )
        indices += line_parts_length * [self._current_index]
        self._current_index += 1
        _line_cursor = _remaining_lines

        # Recursively fill more files
        while _line_cursor < len(grouped_lines):
            _group_slice = grouped_lines_length[_line_cursor: _line_cursor + self.max_lines]
            line_parts_length = sum(
                [length for length in _group_slice]
            )
            indices += line_parts_length * [self._current_index]
            self.current_line_count = len(_group_slice)
            if self.current_line_count >= self.max_lines:
                self._current_index += 1
            _line_cursor += len(_group_slice)

        # Correct current line count if last line was incomplete
        if grouped_lines and not grouped_lines[-1][-1].endswith(self.delimiter):
            self.current_line_count -= 1

        return indices
