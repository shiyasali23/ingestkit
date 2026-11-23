import hashlib

from ingestkit.batch_planner import TransferBatch
from ingestkit.clinical_reader import ClinicalReader
from ingestkit.file_inventory import FileInventory
from ingestkit.file_transformer import FileTransformer
from ingestkit.genomics_reader import GenomicsReader
from ingestkit.local_storage import LocalStorage
from ingestkit.pipeline_errors import RetryableError
from ingestkit.privacy_filter import PrivacyFilter
from ingestkit.slide_reader import SlideReader
from ingestkit.state_schema import FILE_DONE, FILE_FAILED, FILE_QUARANTINED
from ingestkit.table_writer import TableWriter
from ingestkit.transfer_worker import TransferWorker
from tests.conftest import TEST_SALT


class CorruptingStorage(LocalStorage):
    def write_from_stream(self, relative_path, source_stream, chunk_size):
        bytes_written = super().write_from_stream(relative_path, source_stream, chunk_size)
        with open(self._resolve_path(relative_path), "r+b") as written_file:
            written_file.write(b"CORRUPT")
        return bytes_written


class FlakyStorage(LocalStorage):
    def __init__(self, root_path, failures_before_success):
        super().__init__(root_path)
        self.remaining_failures = failures_before_success

    def write_from_stream(self, relative_path, source_stream, chunk_size):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise RetryableError("Simulated network timeout")
        return super().write_from_stream(relative_path, source_stream, chunk_size)


def list_stored_files(folder_path) -> list:
    if not folder_path.exists():
        return []
    return [file_path for file_path in folder_path.rglob("*") if file_path.is_file()]


def build_worker(source_folder, destination_storage, retry_limit=2) -> TransferWorker:
    file_transformer = FileTransformer(
        destination_storage=destination_storage,
        table_writer=TableWriter(destination_storage, chunk_size_bytes=1024),
        slide_reader=SlideReader(),
        genomics_reader=GenomicsReader(min_variant_depth=100, min_variant_vaf=0.05),
        clinical_reader=ClinicalReader(["case_id", "age", "sex", "diagnosis"]),
        privacy_filter=PrivacyFilter(TEST_SALT, ["name", "birth_date"]),
    )
    return TransferWorker(
        source_storage=LocalStorage(str(source_folder)),
        destination_storage=destination_storage,
        file_transformer=file_transformer,
        retry_limit=retry_limit,
        retry_delay_seconds=0,
        chunk_size_bytes=1024,
        run_id="run-test",
    )


def build_batch(source_folder) -> TransferBatch:
    manifest_entries = FileInventory("^([^._]+)").build_manifest(LocalStorage(str(source_folder)))
    return TransferBatch(batch_id="batch-00001", manifest_entries=manifest_entries)


def test_batch_is_copied_and_verified(source_folder, destination_folder):
    transfer_worker = build_worker(source_folder, LocalStorage(str(destination_folder)))
    batch_result = transfer_worker.run_batch(build_batch(source_folder))

    assert batch_result.batch_succeeded
    for file_result in batch_result.file_results:
        assert file_result.file_status == FILE_DONE
        source_bytes = (source_folder / file_result.manifest_entry.relative_path).read_bytes()
        destination_bytes = (destination_folder / file_result.destination_path).read_bytes()
        assert destination_bytes == source_bytes
        assert file_result.source_checksum == hashlib.sha256(source_bytes).hexdigest()
    assert list_stored_files(destination_folder / "staging") == []


def test_output_is_named_by_checksum(source_folder, destination_folder):
    batch_result = build_worker(source_folder, LocalStorage(str(destination_folder))).run_batch(
        build_batch(source_folder)
    )
    slide_result = next(
        result for result in batch_result.file_results if result.manifest_entry.file_type == "slide"
    )
    assert slide_result.destination_path == f"raw/slide/{slide_result.source_checksum}.svs"


def test_source_files_are_never_changed(source_folder, destination_folder):
    files_before = {path.name: path.read_bytes() for path in source_folder.rglob("*") if path.is_file()}
    build_worker(source_folder, LocalStorage(str(destination_folder))).run_batch(build_batch(source_folder))
    files_after = {path.name: path.read_bytes() for path in source_folder.rglob("*") if path.is_file()}
    assert files_before == files_after


def test_unknown_and_empty_files_are_quarantined(tmp_path, destination_folder):
    source_folder = tmp_path / "problem_source"
    source_folder.mkdir()
    (source_folder / "notes.docx").write_bytes(b"unsupported")
    (source_folder / "C-0009.vcf").write_bytes(b"")

    batch_result = build_worker(source_folder, LocalStorage(str(destination_folder))).run_batch(
        build_batch(source_folder)
    )
    reasons_by_path = {result.manifest_entry.relative_path: result for result in batch_result.file_results}

    assert reasons_by_path["notes.docx"].file_status == FILE_QUARANTINED
    assert "Unsupported file type" in reasons_by_path["notes.docx"].status_reason
    assert reasons_by_path["C-0009.vcf"].status_reason == "Empty file"
    assert (destination_folder / "quarantine" / "notes.docx").exists()
    assert batch_result.batch_succeeded


def test_corrupted_copy_is_retried_then_failed(source_folder, destination_folder):
    transfer_worker = build_worker(source_folder, CorruptingStorage(str(destination_folder)), retry_limit=2)
    batch_result = transfer_worker.run_batch(build_batch(source_folder))

    assert not batch_result.batch_succeeded
    assert all(result.file_status == FILE_FAILED for result in batch_result.file_results)
    assert "Checksum mismatch" in batch_result.file_results[0].status_reason
    assert list_stored_files(destination_folder / "raw") == []
    assert list_stored_files(destination_folder / "staging") == []


def test_temporary_error_recovers_on_retry(source_folder, destination_folder):
    flaky_storage = FlakyStorage(str(destination_folder), failures_before_success=2)
    transfer_worker = build_worker(source_folder, flaky_storage, retry_limit=2)
    single_file_batch = build_batch(source_folder)
    single_file_batch.manifest_entries = single_file_batch.manifest_entries[:1]

    batch_result = transfer_worker.run_batch(single_file_batch)
    assert batch_result.file_results[0].file_status == FILE_DONE


def test_second_copy_of_same_file_is_not_duplicated(source_folder, destination_folder):
    transfer_worker = build_worker(source_folder, LocalStorage(str(destination_folder)))
    transfer_worker.run_batch(build_batch(source_folder))
    second_result = transfer_worker.run_batch(build_batch(source_folder))

    assert all(
        result.status_reason == "Identical file already stored" for result in second_result.file_results
    )
    assert len(list_stored_files(destination_folder / "raw")) == 6
