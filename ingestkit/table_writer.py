import io

import pyarrow as pa
import pyarrow.parquet as pq

from ingestkit.pipeline_errors import QuarantineError, RetryableError
from ingestkit.storage_backend import StorageBackend

VARIANT_TABLE_SCHEMA = pa.schema(
    [
        ("case_id", pa.string()),
        ("gene", pa.string()),
        ("chromosome", pa.string()),
        ("position", pa.int64()),
        ("reference_allele", pa.string()),
        ("alternate_allele", pa.string()),
        ("filter_status", pa.string()),
        ("read_depth", pa.int64()),
        ("allele_fraction", pa.float64()),
        ("source_sha256", pa.string()),
    ]
)


class TableWriter:
    def __init__(self, destination_storage: StorageBackend, chunk_size_bytes: int):
        self.destination_storage = destination_storage
        self.chunk_size_bytes = chunk_size_bytes

    def write_table(
        self, table_rows: list[dict], relative_path: str, table_schema: pa.Schema | None = None
    ) -> int:
        if not table_rows and table_schema is None:
            raise ValueError("An empty table needs a schema")

        try:
            arrow_table = pa.Table.from_pylist(table_rows, schema=table_schema)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            raise QuarantineError(f"Rows do not fit the table schema: {e}") from e

        parquet_buffer = io.BytesIO()
        pq.write_table(arrow_table, parquet_buffer)
        parquet_buffer.seek(0)
        self.destination_storage.write_from_stream(relative_path, parquet_buffer, self.chunk_size_bytes)
        return arrow_table.num_rows

    def read_table(self, relative_path: str) -> list[dict]:
        with self.destination_storage.open_reader(relative_path) as table_stream:
            table_bytes = table_stream.read()
        try:
            return pq.read_table(io.BytesIO(table_bytes)).to_pylist()
        except pa.ArrowException as e:
            raise RetryableError(f"Error reading table {relative_path}: {e}") from e
