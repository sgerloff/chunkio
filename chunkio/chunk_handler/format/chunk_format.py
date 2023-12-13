from abc import ABC, abstractmethod
from typing import Tuple, Union, Generator


class BaseChunkFormat(ABC):
    @abstractmethod
    def format(self, file_path: str, index: int) -> str:
        """
        Formats chunk file paths, e.g. for input

        file_path: /path/to/base_file.txt
        index: 0

        - /path/to/base_file.txt/base_file.0.txt
        - /path/to/base_file/000000_base_file.txt
        - ...

        :param file_path: base file path
        :param index: positive integer index of chunk
        :return: unique file path for chunk with specified index
        """
        pass

    @abstractmethod
    def parse(self, chunk_file_path: str) -> Tuple[str, int]:
        """
        Interpret chunked file path. Extracts base file path and index corresponding to the chunked file path.

        :param chunk_file_path: path to chunked file
        :return: (base file path, index)
        """
        pass

    @abstractmethod
    def walk(self, file_path: str, return_index: bool = False) -> Generator[Union[str, Tuple[str, int]], None, None]:
        """
        Generator yielding all chunked file paths corresponding to the base file path in ascending order.
        Optionally, does return the index.

        :param file_path: path to base file
        :param return_index: if true does yield a tuple with the chunk file path and the corresponding index
        :return: either chunked file path or tuple of chunked file path and corresponding index
        """
        pass
