import os

from abc import ABC, abstractmethod
from typing import Tuple, Union, Generator


class BaseChunkFormat(ABC):
    @staticmethod
    def _validate_index(index: int):
        assert isinstance(index, int) and index >= 0, "Index should be an unsigned positive integer!"

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
    def list(self, file_path: str, return_index: bool = False) -> Generator[Union[str, Tuple[str, int]], None, None]:
        """
        Generator yielding all chunked file paths corresponding to the base file path in ascending order.
        Optionally, does return the index.

        :param file_path: path to base file
        :param return_index: if true does yield a tuple with the chunk file path and the corresponding index
        :return: either chunked file path or tuple of chunked file path and corresponding index
        """
        pass


class SubdirNumberedChunkFormat(BaseChunkFormat):
    def __init__(self, index_format: str = "06d", keep_extension: bool = True):
        """
        Uses the base file path as a directory and places chunks in that directory:

        keep_extension: true -> /path/to/base_file_path.ext/base_file_path.{index:06d}.ext
        keep_extension: false -> /path/to/base_file_path/base_file_path.{index:06d}.ext

        :param index_format: format string for the index string
        :param keep_extension: keeps the file extension when creating the directory containing the file chunks, default true
        """

        self.index_format = index_format
        self.keep_extension = keep_extension

    def format(self, file_path: str, index: int) -> str:
        self._validate_index(index)

        file_name = os.path.basename(file_path)
        file_name, file_ext = os.path.splitext(file_name)

        if not self.keep_extension:
            file_path, _ = os.path.splitext(file_path)

        if not os.path.isdir(file_path):
            os.mkdir(file_path)

        return os.path.join(file_path, f"{file_name}.{index:{self.index_format}}{file_ext}")

    def parse(self, chunk_file_path: str) -> Tuple[str, int]:
        base_file_path = os.path.dirname(chunk_file_path)

        if not self.keep_extension:
            _, file_extension = os.path.splitext(chunk_file_path)
            base_file_path = base_file_path + file_extension

        _chunk_file_name = os.path.basename(chunk_file_path)

        _chunk_file_split = _chunk_file_name.split(".")
        assert len(_chunk_file_split) >= 3, f"Unexpected format from {chunk_file_path}"
        try:
            chunk_index = int(_chunk_file_split[-2])
        except ValueError:
            raise AssertionError(f"Could not parse index from {chunk_file_path}")
        self._validate_index(chunk_index)

        return base_file_path, chunk_index

    def list(self, file_path: str, return_index: bool = False) -> Generator[Union[str, Tuple[str, int]], None, None]:
        parsed_files, parsed_ids = [],[]

        expected_base_path = file_path
        if not self.keep_extension:
            file_path, _ = os.path.splitext(file_path)

        for file_name in os.listdir(file_path):
            potential_file_path = os.path.join(file_path, file_name)
            if not os.path.isfile(potential_file_path):
                continue
            try:
                base_path, index = self.parse(potential_file_path)
            except AssertionError:
                continue
            if base_path != expected_base_path:
                continue
            parsed_files.append(potential_file_path)
            parsed_ids.append(index)

        sorted_file_paths = [(path, index) for path, index in sorted(zip(parsed_files, parsed_ids), key=lambda t: t[1])]
        for chunk_file, index in sorted_file_paths:
            if return_index:
                yield chunk_file, index
            else:
                yield chunk_file
