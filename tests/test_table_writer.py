import pytest

from ingestkit.local_storage import LocalStorage
from ingestkit.table_writer import VARIANT_TABLE_SCHEMA, TableWriter


def test_rows_round_trip_through_parquet(tmp_path):
    table_writer = TableWriter(LocalStorage(str(tmp_path)), chunk_size_bytes=1024)
    table_rows = [{"case_id": "C-0001", "age": 71}, {"case_id": "C-0002", "age": 64}]

    written_count = table_writer.write_table(table_rows, "processed/clinical/sample.parquet")

    assert written_count == 2
    assert table_writer.read_table("processed/clinical/sample.parquet") == table_rows


def test_empty_table_keeps_its_columns(tmp_path):
    table_writer = TableWriter(LocalStorage(str(tmp_path)), chunk_size_bytes=1024)
    table_writer.write_table([], "processed/genomics/empty.parquet", VARIANT_TABLE_SCHEMA)
    assert table_writer.read_table("processed/genomics/empty.parquet") == []


def test_empty_table_without_schema_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        TableWriter(LocalStorage(str(tmp_path)), chunk_size_bytes=1024).write_table([], "empty.parquet")
