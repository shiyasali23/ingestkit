from collections import defaultdict

from ingestkit.pipeline_logging import get_pipeline_logger
from ingestkit.table_writer import TableWriter

CASE_TABLE_PATH = "processed/cases/cases.parquet"
REQUIRED_FILE_TYPES = ("slide", "genomics", "clinical")

module_logger = get_pipeline_logger(__name__)


class CaseLinker:
    def __init__(self, table_writer: TableWriter, gene_panel: list[str]):
        self.table_writer = table_writer
        self.gene_panel = gene_panel

    def build_case_rows(self, ingested_files: list[dict]) -> list[dict]:
        files_by_case = defaultdict(list)
        for ingested_file in ingested_files:
            files_by_case[ingested_file["case_id"]].append(ingested_file)

        return [self._build_single_case(case_id, files_by_case[case_id]) for case_id in sorted(files_by_case)]

    def write_case_table(self, case_rows: list[dict]) -> int:
        if not case_rows:
            return 0
        return self.table_writer.write_table(case_rows, CASE_TABLE_PATH)

    def _build_single_case(self, case_id: str, case_files: list[dict]) -> dict:
        files_by_type = defaultdict(list)
        for case_file in case_files:
            files_by_type[case_file["file_type"]].append(case_file)

        missing_types = [file_type for file_type in REQUIRED_FILE_TYPES if not files_by_type[file_type]]
        variant_rows = self._read_rows(files_by_type["genomics"])
        clinical_rows = self._read_rows(files_by_type["clinical"])
        mutated_genes = {variant_row["gene"] for variant_row in variant_rows}

        case_row = {
            "case_id": case_id,
            "slide_count": len(files_by_type["slide"]),
            "genomics_count": len(files_by_type["genomics"]),
            "clinical_count": len(files_by_type["clinical"]),
            "is_complete": not missing_types,
            "missing_parts": ",".join(missing_types),
            "slide_raw_path": files_by_type["slide"][0]["destination_path"]
            if files_by_type["slide"]
            else None,
            "diagnosis": clinical_rows[0].get("diagnosis") if clinical_rows else None,
            "mutation_count": len(variant_rows),
            "max_allele_fraction": max(
                (variant_row["allele_fraction"] for variant_row in variant_rows), default=0.0
            ),
        }
        for gene_name in self.gene_panel:
            case_row[f"{gene_name}_mutated"] = gene_name in mutated_genes
        return case_row

    def _read_rows(self, case_files: list[dict]) -> list[dict]:
        table_rows = []
        for case_file in case_files:
            if not case_file.get("processed_path"):
                continue
            try:
                table_rows.extend(self.table_writer.read_table(case_file["processed_path"]))
            except Exception as e:
                module_logger.error(f"Error reading processed table {case_file['processed_path']}: {e}")
        return table_rows
