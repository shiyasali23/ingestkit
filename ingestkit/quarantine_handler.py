from ingestkit.file_inventory import ManifestEntry
from ingestkit.pipeline_errors import PipelineError
from ingestkit.pipeline_logging import get_pipeline_logger
from ingestkit.storage_backend import StorageBackend

QUARANTINE_FOLDER = "quarantine"

module_logger = get_pipeline_logger(__name__)


class QuarantineHandler:
    def __init__(
        self, source_storage: StorageBackend, destination_storage: StorageBackend, chunk_size_bytes: int
    ):
        self.source_storage = source_storage
        self.destination_storage = destination_storage
        self.chunk_size_bytes = chunk_size_bytes

    def quarantine_file(self, manifest_entry: ManifestEntry, quarantine_reason: str) -> str | None:
        quarantine_path = f"{QUARANTINE_FOLDER}/{manifest_entry.relative_path}"
        try:
            with self.source_storage.open_reader(manifest_entry.relative_path) as source_stream:
                self.destination_storage.write_from_stream(
                    quarantine_path, source_stream, self.chunk_size_bytes
                )
        except PipelineError as e:
            module_logger.error(
                f"Error copying file to quarantine: {e}",
                extra={"source_path": manifest_entry.relative_path},
            )
            return None

        module_logger.warning(
            f"File quarantined: {quarantine_reason}",
            extra={"source_path": manifest_entry.relative_path},
        )
        return quarantine_path

    def quarantine_staged_file(
        self, manifest_entry: ManifestEntry, staged_path: str, quarantine_reason: str
    ) -> str:
        quarantine_path = f"{QUARANTINE_FOLDER}/{manifest_entry.relative_path}"
        self.destination_storage.move_file(staged_path, quarantine_path)
        module_logger.warning(
            f"File quarantined after copy: {quarantine_reason}",
            extra={"source_path": manifest_entry.relative_path},
        )
        return quarantine_path
