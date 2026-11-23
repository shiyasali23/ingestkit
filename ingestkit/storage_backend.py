from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import BinaryIO


@dataclass(frozen=True)
class StoredFile:
    relative_path: str
    size_bytes: int


class StorageBackend(ABC):
    @abstractmethod
    def list_files(self) -> Iterator[StoredFile]: ...

    @abstractmethod
    def open_reader(self, relative_path: str) -> BinaryIO: ...

    @abstractmethod
    def write_from_stream(self, relative_path: str, source_stream: BinaryIO, chunk_size: int) -> int: ...

    @abstractmethod
    def move_file(self, source_relative_path: str, target_relative_path: str) -> None: ...

    @abstractmethod
    def delete_file(self, relative_path: str) -> None: ...

    @abstractmethod
    def delete_folder(self, relative_path: str) -> None: ...

    @abstractmethod
    def file_exists(self, relative_path: str) -> bool: ...

    @abstractmethod
    def describe_location(self, relative_path: str) -> str: ...
