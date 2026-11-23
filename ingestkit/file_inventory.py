import re
from dataclasses import dataclass
from pathlib import PurePosixPath

from ingestkit.storage_backend import StorageBackend

UNKNOWN_FILE_TYPE = "unknown"
UNKNOWN_CASE_ID = "unknown"

FILE_TYPE_BY_EXTENSION = {
    ".svs": "slide",
    ".ndpi": "slide",
    ".tif": "slide",
    ".tiff": "slide",
    ".vcf": "genomics",
    ".vcf.gz": "genomics",
    ".json": "clinical",
    ".csv": "clinical",
}


@dataclass(frozen=True)
class ManifestEntry:
    relative_path: str
    size_bytes: int
    file_type: str
    file_extension: str
    case_id: str


class FileInventory:
    def __init__(self, case_id_pattern: str):
        self.case_id_regex = re.compile(case_id_pattern)

    def build_manifest(self, source_storage: StorageBackend) -> list[ManifestEntry]:
        manifest_entries = []
        for stored_file in source_storage.list_files():
            file_extension = self.detect_file_extension(stored_file.relative_path)
            manifest_entries.append(
                ManifestEntry(
                    relative_path=stored_file.relative_path,
                    size_bytes=stored_file.size_bytes,
                    file_type=FILE_TYPE_BY_EXTENSION.get(file_extension, UNKNOWN_FILE_TYPE),
                    file_extension=file_extension,
                    case_id=self.extract_case_id(stored_file.relative_path),
                )
            )
        return manifest_entries

    def extract_case_id(self, relative_path: str) -> str:
        file_name = PurePosixPath(relative_path).name
        case_match = self.case_id_regex.search(file_name)
        if not case_match or not case_match.group(1):
            return UNKNOWN_CASE_ID
        return case_match.group(1)

    @staticmethod
    def detect_file_extension(relative_path: str) -> str:
        lower_name = PurePosixPath(relative_path).name.lower()
        for known_extension in sorted(FILE_TYPE_BY_EXTENSION, key=len, reverse=True):
            if lower_name.endswith(known_extension):
                return known_extension
        return PurePosixPath(lower_name).suffix
