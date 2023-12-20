from abc import ABC, abstractmethod
from typing import List


class SequentialChunker(ABC):

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

    @abstractmethod
    def indices(self, lines: List[str]) -> List[int]:
        """
        Split list of lines into chunks of lines written to the next chunk files.

        :param lines: list of lines (each being a string)
        :return: list of chunk indices for the provided list of lines
        """
        pass


class MaxLineSequentialChunker(SequentialChunker):
    def __init__(self, max_lines: int = 10_000):
        self.max_lines = max_lines
        self.current_line_count = None
        self.current_index = None
        self.reset()

    def reset(self):
        self.current_line_count = 0
        self.current_index = 0

    def index(self, line: str) -> int:
        if self.current_line_count >= self.max_lines:
            self.current_line_count = 0  # Reset line count
            self.current_index += 1

        self.current_line_count += 1
        return self.current_index

    def indices(self, lines: List[str]) -> List[int]:
        _remaining_lines = self.max_lines - self.current_line_count
        _lines_length = len(lines)

        indices = []
        if _remaining_lines > _lines_length:
            self.current_line_count += _lines_length
            return _lines_length * [self.current_index]
        else:
            indices += _remaining_lines * [self.current_index]
            self.current_index += 1
            _lines_length -= _remaining_lines

        _num_chunks = int(_lines_length/self.max_lines)
        _resulting_line_count = _lines_length % self.max_lines
        for _ in range(_num_chunks):
            indices += self.max_lines * [self.current_index]
            self.current_index += 1
        indices += _resulting_line_count * [self.current_index]
        self.current_line_count = _resulting_line_count
        return indices