import os
import shutil
import re

from typing import Optional


class FileSystemBuilder:
    def __init__(
            self,
            data: str,
            base_path: Optional[str] = None,
            keep_files: bool = False,
            parse_file_names_regex: str = "\<([^<>]*)\>"
    ):
        self.data = data
        self.base_path = base_path
        self.keep_files = keep_files
        self.parse_file_names_regex = parse_file_names_regex

        if base_path is None:
            assert keep_files, "Cleaning up is not supported without a base_path being set!"

    def __enter__(self):
        parsed_data = re.split(self.parse_file_names_regex, self.data)
        current_file = None
        for i, part in enumerate(parsed_data):
            if i == 0:
                continue
            if i % 2 != 0:
                if current_file:
                    current_file.close()
                if self.base_path:
                    file_path = os.path.join(self.base_path, part)
                else:
                    file_path = part
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                current_file = open(file_path, "w")
            else:
                current_file.write(part)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.keep_files:
            return None

        if self.base_path:
            shutil.rmtree(self.base_path, ignore_errors=True)
