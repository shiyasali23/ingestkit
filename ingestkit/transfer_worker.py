import time
import uuid
from dataclasses import dataclass, field

from ingestkit.batch_planner import TransferBatch
from ingestkit.file_checksum import ChecksumReader, calculate_stream_checksum
from ingestkit.file_inventory import UNKNOWN_FILE_TYPE, ManifestEntry
from ingestkit.file_transformer import FileTransformer
from ingestkit.pipeline_errors import FatalError, QuarantineError, RetryableError
from ingestkit.pipeline_logging import configure_pipeline_logging, get_pipeline_logger
from ingestkit.quarantine_handler import QuarantineHandler
from ingestkit.state_schema import FILE_DONE, FILE_FAILED, FILE_QUARANTINED
from ingestkit.storage_backend import StorageBackend

RAW_FOLDER = "raw"
STAGING_FOLDER = "staging"
ALREADY_STORED_REASON = "Identical file already stored"


def build_staging_folder(run_id: str) -> str:
    return f"{STAGING_FOLDER}/{run_id or 'manual'}"


@dataclass
class FileResult:
    manifest_entry: ManifestEntry
    file_status: str
    source_checksum: str | None = None
    destination_path: str | None = None
    processed_path: str | None = None
    status_reason: str | None = None


@dataclass
class BatchResult:
    batch_id: str
    file_results: list[FileResult] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def batch_succeeded(self) -> bool:
        return all(file_result.file_status != FILE_FAILED for file_result in self.file_results)

    @property
    def transferred_bytes(self) -> int:
        return sum(
            file_result.manifest_entry.size_bytes
            for file_result in self.file_results
            if file_result.file_status == FILE_DONE
        )


class TransferWorker:
    def __init__(
        self,
        source_storage: StorageBackend,
        destination_storage: StorageBackend,
        file_transformer: FileTransformer,
        retry_limit: int,
        retry_delay_seconds: float,
        chunk_size_bytes: int,
        run_id: str = "",
        log_level: str = "INFO",
    ):
        self.source_storage = source_storage
        self.destination_storage = destination_storage
        self.file_transformer = file_transformer
        self.retry_limit = retry_limit
        self.retry_delay_seconds = retry_delay_seconds
        self.chunk_size_bytes = chunk_size_bytes
        self.run_id = run_id
        self.log_level = log_level
        self.staging_folder = build_staging_folder(run_id)
        self.quarantine_handler = QuarantineHandler(source_storage, destination_storage, chunk_size_bytes)

    def run_batch(self, transfer_batch: TransferBatch) -> BatchResult:
        configure_pipeline_logging(self.log_level)
        batch_logger = get_pipeline_logger(__name__, run_id=self.run_id, batch_id=transfer_batch.batch_id)
        batch_logger.info(f"Batch started with {transfer_batch.file_count} files")

        start_time = time.monotonic()
        batch_result = BatchResult(batch_id=transfer_batch.batch_id)
        for manifest_entry in transfer_batch.manifest_entries:
            batch_result.file_results.append(self.transfer_single_file(manifest_entry))
        batch_result.duration_seconds = round(time.monotonic() - start_time, 3)

        batch_logger.info(
            f"Batch finished in {batch_result.duration_seconds}s, succeeded={batch_result.batch_succeeded}"
        )
        return batch_result

    def transfer_single_file(self, manifest_entry: ManifestEntry) -> FileResult:
        file_logger = get_pipeline_logger(
            __name__, run_id=self.run_id, source_path=manifest_entry.relative_path
        )
        last_error_message = ""

        for attempt_number in range(1, self.retry_limit + 2):
            try:
                file_result = self._process_file(manifest_entry)
                file_logger.info(
                    f"File {file_result.file_status}",
                    extra={"checksum": file_result.source_checksum, "file_status": file_result.file_status},
                )
                return file_result
            except QuarantineError as e:
                return self._quarantine_from_source(manifest_entry, str(e))
            except FatalError as e:
                file_logger.error(f"Fatal error transferring file: {e}")
                return FileResult(
                    manifest_entry=manifest_entry, file_status=FILE_FAILED, status_reason=str(e)
                )
            except RetryableError as e:
                last_error_message = str(e)
                file_logger.warning(f"Retryable error: {e}", extra={"attempt_number": attempt_number})
                if attempt_number <= self.retry_limit:
                    time.sleep(self.retry_delay_seconds * 2 ** (attempt_number - 1))
            except Exception as e:
                file_logger.error(f"Unexpected error transferring file: {e}", exc_info=True)
                return FileResult(
                    manifest_entry=manifest_entry, file_status=FILE_FAILED, status_reason=str(e)
                )

        return FileResult(
            manifest_entry=manifest_entry,
            file_status=FILE_FAILED,
            status_reason=f"Gave up after {self.retry_limit + 1} attempts: {last_error_message}",
        )

    def _process_file(self, manifest_entry: ManifestEntry) -> FileResult:
        if manifest_entry.file_type == UNKNOWN_FILE_TYPE:
            raise QuarantineError(f"Unsupported file type: {manifest_entry.file_extension or 'no extension'}")
        if manifest_entry.size_bytes == 0:
            raise QuarantineError("Empty file")

        staging_path = f"{self.staging_folder}/{uuid.uuid4().hex}{manifest_entry.file_extension}"
        try:
            source_checksum = self._copy_to_staging(manifest_entry, staging_path)
            self._verify_staged_copy(staging_path, source_checksum)

            try:
                transform_result = self.file_transformer.transform_file(
                    manifest_entry, staging_path, source_checksum
                )
            except QuarantineError as e:
                return self._quarantine_staged_copy(manifest_entry, staging_path, source_checksum, str(e))

            raw_path, status_reason = self._store_raw_copy(manifest_entry, staging_path, source_checksum)
            return FileResult(
                manifest_entry=manifest_entry,
                file_status=FILE_DONE,
                source_checksum=source_checksum,
                destination_path=raw_path,
                processed_path=transform_result.processed_path,
                status_reason=status_reason,
            )
        except Exception:
            self._remove_staging_file(staging_path)
            raise

    def _store_raw_copy(
        self, manifest_entry: ManifestEntry, staging_path: str, source_checksum: str
    ) -> tuple[str, str | None]:
        raw_path = f"{RAW_FOLDER}/{manifest_entry.file_type}/{source_checksum}{manifest_entry.file_extension}"
        if self.destination_storage.file_exists(raw_path):
            self.destination_storage.delete_file(staging_path)
            return raw_path, ALREADY_STORED_REASON
        self.destination_storage.move_file(staging_path, raw_path)
        return raw_path, None

    def _quarantine_staged_copy(
        self,
        manifest_entry: ManifestEntry,
        staging_path: str,
        source_checksum: str,
        quarantine_reason: str,
    ) -> FileResult:
        quarantine_path = self.quarantine_handler.quarantine_staged_file(
            manifest_entry, staging_path, quarantine_reason
        )
        return FileResult(
            manifest_entry=manifest_entry,
            file_status=FILE_QUARANTINED,
            source_checksum=source_checksum,
            destination_path=quarantine_path,
            status_reason=quarantine_reason,
        )

    def _copy_to_staging(self, manifest_entry: ManifestEntry, staging_path: str) -> str:
        with self.source_storage.open_reader(manifest_entry.relative_path) as source_stream:
            checksum_reader = ChecksumReader(source_stream)
            bytes_written = self.destination_storage.write_from_stream(
                staging_path, checksum_reader, self.chunk_size_bytes
            )

        expected_bytes = manifest_entry.size_bytes
        if bytes_written != expected_bytes:
            raise RetryableError(
                f"File size changed during transfer: expected {expected_bytes}, copied {bytes_written}"
            )
        return checksum_reader.checksum_value

    def _verify_staged_copy(self, staging_path: str, source_checksum: str) -> None:
        with self.destination_storage.open_reader(staging_path) as staged_stream:
            staged_checksum = calculate_stream_checksum(staged_stream, self.chunk_size_bytes)
        if staged_checksum != source_checksum:
            raise RetryableError("Checksum mismatch after copy")

    def _quarantine_from_source(self, manifest_entry: ManifestEntry, quarantine_reason: str) -> FileResult:
        quarantine_path = self.quarantine_handler.quarantine_file(manifest_entry, quarantine_reason)
        return FileResult(
            manifest_entry=manifest_entry,
            file_status=FILE_QUARANTINED,
            destination_path=quarantine_path,
            status_reason=quarantine_reason,
        )

    def _remove_staging_file(self, staging_path: str) -> None:
        try:
            self.destination_storage.delete_file(staging_path)
        except Exception as e:
            get_pipeline_logger(__name__, run_id=self.run_id).warning(f"Error removing staging file: {e}")
