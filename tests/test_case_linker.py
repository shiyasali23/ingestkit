from ingestkit.case_linker import CASE_TABLE_PATH, CaseLinker
from ingestkit.local_storage import LocalStorage
from ingestkit.table_writer import VARIANT_TABLE_SCHEMA, TableWriter


def build_variant_row(case_id: str, gene_name: str, allele_fraction: float) -> dict:
    return {
        "case_id": case_id,
        "gene": gene_name,
        "chromosome": "chr2",
        "position": 100,
        "reference_allele": "T",
        "alternate_allele": "C",
        "filter_status": "PASS",
        "read_depth": 500,
        "allele_fraction": allele_fraction,
        "source_sha256": "a" * 64,
    }


def build_ingested_file(case_id: str, file_type: str, processed_path: str) -> dict:
    return {
        "case_id": case_id,
        "file_type": file_type,
        "source_checksum": f"{case_id}-{file_type}",
        "destination_path": f"raw/{file_type}/{case_id}.bin",
        "processed_path": processed_path,
    }


def test_complete_case_gets_gene_labels(tmp_path):
    table_writer = TableWriter(LocalStorage(str(tmp_path)), chunk_size_bytes=1024)
    table_writer.write_table(
        [build_variant_row("C-0042", "SF3B1", 0.38), build_variant_row("C-0042", "TET2", 0.12)],
        "processed/genomics/C-0042.parquet",
        VARIANT_TABLE_SCHEMA,
    )
    table_writer.write_table([{"case_id": "C-0042", "diagnosis": "MDS"}], "processed/clinical/C-0042.parquet")
    ingested_files = [
        build_ingested_file("C-0042", "slide", "processed/slide/C-0042.parquet"),
        build_ingested_file("C-0042", "genomics", "processed/genomics/C-0042.parquet"),
        build_ingested_file("C-0042", "clinical", "processed/clinical/C-0042.parquet"),
    ]

    case_rows = CaseLinker(table_writer, ["SF3B1", "TP53", "TET2"]).build_case_rows(ingested_files)

    assert len(case_rows) == 1
    case_row = case_rows[0]
    assert case_row["is_complete"] is True
    assert case_row["diagnosis"] == "MDS"
    assert case_row["mutation_count"] == 2
    assert case_row["max_allele_fraction"] == 0.38
    assert (case_row["SF3B1_mutated"], case_row["TP53_mutated"], case_row["TET2_mutated"]) == (
        True,
        False,
        True,
    )


def test_case_without_genomics_is_marked_incomplete(tmp_path):
    table_writer = TableWriter(LocalStorage(str(tmp_path)), chunk_size_bytes=1024)
    case_rows = CaseLinker(table_writer, ["SF3B1"]).build_case_rows(
        [build_ingested_file("C-0045", "slide", None)]
    )
    assert case_rows[0]["is_complete"] is False
    assert case_rows[0]["missing_parts"] == "genomics,clinical"
    assert case_rows[0]["SF3B1_mutated"] is False


def test_case_table_is_written(tmp_path):
    table_writer = TableWriter(LocalStorage(str(tmp_path)), chunk_size_bytes=1024)
    case_linker = CaseLinker(table_writer, ["SF3B1"])
    case_rows = case_linker.build_case_rows([build_ingested_file("C-0045", "slide", None)])

    assert case_linker.write_case_table(case_rows) == 1
    assert table_writer.read_table(CASE_TABLE_PATH)[0]["case_id"] == "C-0045"
