from dataclasses import dataclass

import pyarrow as pa

from ingestkit.clinical_reader import ClinicalReader
from ingestkit.file_inventory import ManifestEntry
from ingestkit.genomics_reader import GenomicsReader
from ingestkit.pipeline_errors import QuarantineError
from ingestkit.privacy_filter import PrivacyFilter
from ingestkit.slide_reader import SlideReader
from ingestkit.storage_backend import StorageBackend
from ingestkit.table_writer import VARIANT_TABLE_SCHEMA, TableWriter

PROCESSED_FOLDER = "processed"
SHORT_CHECKSUM_LENGTH = 12


@dataclass
class TransformResult:
    processed_path: str
    row_count: int


class FileTransformer:
    def __init__(
        self,
        destination_storage: StorageBackend,
        table_writer: TableWriter,
        slide_reader: SlideReader,
        genomics_reader: GenomicsReader,
        clinical_reader: ClinicalReader,
        privacy_filter: PrivacyFilter,
    ):
        self.destination_storage = destination_storage
        self.table_writer = table_writer
        self.slide_reader = slide_reader
        self.genomics_reader = genomics_reader
        self.clinical_reader = clinical_reader
        self.privacy_filter = privacy_filter

    def transform_file(
        self, manifest_entry: ManifestEntry, staged_path: str, source_checksum: str
    ) -> TransformResult:
        table_rows, table_schema = self._read_table_rows(manifest_entry, staged_path)
        for table_row in table_rows:
            table_row["source_sha256"] = source_checksum

        processed_path = self.build_processed_path(manifest_entry, source_checksum)
        row_count = self.table_writer.write_table(table_rows, processed_path, table_schema)
        return TransformResult(processed_path=processed_path, row_count=row_count)

    @staticmethod
    def build_processed_path(manifest_entry: ManifestEntry, source_checksum: str) -> str:
        short_checksum = source_checksum[:SHORT_CHECKSUM_LENGTH]
        return (
            f"{PROCESSED_FOLDER}/{manifest_entry.file_type}/{manifest_entry.case_id}_{short_checksum}.parquet"
        )

    def _read_table_rows(
        self, manifest_entry: ManifestEntry, staged_path: str
    ) -> tuple[list[dict], pa.Schema | None]:
        case_id = manifest_entry.case_id
        file_extension = manifest_entry.file_extension

        with self.destination_storage.open_reader(staged_path) as staged_stream:
            if manifest_entry.file_type == "slide":
                return [self.slide_reader.read_slide_metadata(staged_stream, case_id)], None

            if manifest_entry.file_type == "genomics":
                genomics_result = self.genomics_reader.read_variants(staged_stream, file_extension, case_id)
                return genomics_result.variant_rows, VARIANT_TABLE_SCHEMA

            if manifest_entry.file_type == "clinical":
                clinical_record = self.clinical_reader.read_clinical_record(
                    staged_stream, file_extension, case_id
                )
                return [self.privacy_filter.deidentify_record(clinical_record)], None

        raise QuarantineError(f"No transformer for file type: {manifest_entry.file_type}")
