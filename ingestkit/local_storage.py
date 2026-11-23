import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from ingestkit.pipeline_errors import FatalError, QuarantineError, RetryableError
from ingestkit.storage_backend import StorageBackend, StoredFile

PARTIAL_FILE_SUFFIX = ".partial"


class LocalStorage(StorageBackend):
    def __init__(self, root_path: str):
        self.root_path = Path(root_path).resolve()

    def list_files(self) -> Iterator[StoredFile]:
        if not self.root_path.is_dir():
            raise FatalError(f"Storage folder not found: {self.root_path}")

        for folder_path, folder_names, file_names in os.walk(self.root_path):
            folder_names[:] = sorted(
                folder_name for folder_name in folder_names if not folder_name.startswith(".")
            )
            for file_name in sorted(file_names):
                if file_name.startswith(".") or file_name.endswith(PARTIAL_FILE_SUFFIX):
                    continue
                absolute_path = Path(folder_path) / file_name
                relative_path = absolute_path.relative_to(self.root_path).as_posix()
                yield StoredFile(relative_path=relative_path, size_bytes=absolute_path.stat().st_size)

    def open_reader(self, relative_path: str) -> BinaryIO:
        file_path = self._resolve_path(relative_path)
        try:
            return open(file_path, "rb")
        except OSError as e:
            raise self._translate_os_error(e, f"Error opening {file_path}") from e

    def write_from_stream(self, relative_path: str, source_stream: BinaryIO, chunk_size: int) -> int:
        target_path = self._resolve_path(relative_path)
        partial_path = target_path.with_name(target_path.name + PARTIAL_FILE_SUFFIX)
        bytes_written = 0
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with open(partial_path, "wb") as partial_file:
                while file_chunk := source_stream.read(chunk_size):
                    partial_file.write(file_chunk)
                    bytes_written += len(file_chunk)
            os.replace(partial_path, target_path)
            return bytes_written
        except OSError as e:
            partial_path.unlink(missing_ok=True)
            raise self._translate_os_error(e, f"Error writing {target_path}") from e

    def move_file(self, source_relative_path: str, target_relative_path: str) -> None:
        source_path = self._resolve_path(source_relative_path)
        target_path = self._resolve_path(target_relative_path)
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source_path, target_path)
        except OSError as e:
            raise self._translate_os_error(e, f"Error moving {source_path} to {target_path}") from e

    def delete_file(self, relative_path: str) -> None:
        file_path = self._resolve_path(relative_path)
        try:
            file_path.unlink(missing_ok=True)
        except OSError as e:
            raise self._translate_os_error(e, f"Error deleting {file_path}") from e

    def delete_folder(self, relative_path: str) -> None:
        folder_path = self._resolve_path(relative_path)
        if folder_path == self.root_path:
            raise FatalError("Refusing to delete the storage root folder")
        try:
            shutil.rmtree(folder_path, ignore_errors=False)
        except FileNotFoundError:
            return
        except OSError as e:
            raise self._translate_os_error(e, f"Error deleting folder {folder_path}") from e

    def file_exists(self, relative_path: str) -> bool:
        return self._resolve_path(relative_path).is_file()

    def describe_location(self, relative_path: str) -> str:
        return str(self._resolve_path(relative_path))

    def _resolve_path(self, relative_path: str) -> Path:
        resolved_path = (self.root_path / relative_path).resolve()
        if not resolved_path.is_relative_to(self.root_path):
            raise FatalError(f"Path escapes storage folder: {relative_path}")
        return resolved_path

    @staticmethod
    def _translate_os_error(os_error: OSError, error_context: str) -> Exception:
        error_message = f"{error_context}: {os_error}"
        if isinstance(os_error, FileNotFoundError):
            return QuarantineError(error_message)
        if isinstance(os_error, PermissionError):
            return FatalError(error_message)
        return RetryableError(error_message)
