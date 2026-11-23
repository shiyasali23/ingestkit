import gzip
import io
from dataclasses import dataclass, field
from typing import BinaryIO

from ingestkit.pipeline_errors import QuarantineError

VCF_FIXED_COLUMNS = ["#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"]
PASSING_FILTER = "PASS"


@dataclass
class GenomicsResult:
    variant_rows: list[dict] = field(default_factory=list)
    total_variant_count: int = 0

    @property
    def kept_variant_count(self) -> int:
        return len(self.variant_rows)


class GenomicsReader:
    def __init__(self, min_variant_depth: int, min_variant_vaf: float):
        self.min_variant_depth = min_variant_depth
        self.min_variant_vaf = min_variant_vaf

    def read_variants(self, vcf_stream: BinaryIO, file_extension: str, case_id: str) -> GenomicsResult:
        text_stream = self._open_text_stream(vcf_stream, file_extension)
        genomics_result = GenomicsResult()
        header_found = False

        for line_number, raw_line in enumerate(text_stream, start=1):
            vcf_line = raw_line.rstrip("\n")
            if line_number == 1 and not vcf_line.startswith("##fileformat=VCF"):
                raise QuarantineError("Not a VCF file: first line must be ##fileformat=VCF")
            if vcf_line.startswith("##") or not vcf_line.strip():
                continue
            if vcf_line.startswith("#CHROM"):
                self._check_header_columns(vcf_line)
                header_found = True
                continue
            if not header_found:
                raise QuarantineError(f"Variant row before #CHROM header at line {line_number}")

            genomics_result.total_variant_count += 1
            variant_row = self._parse_variant_line(vcf_line, line_number, case_id)
            if self._variant_passes_filters(variant_row):
                genomics_result.variant_rows.append(variant_row)

        if not header_found:
            raise QuarantineError("VCF has no #CHROM header line")
        return genomics_result

    def _parse_variant_line(self, vcf_line: str, line_number: int, case_id: str) -> dict:
        line_columns = vcf_line.split("\t")
        if len(line_columns) < len(VCF_FIXED_COLUMNS):
            raise QuarantineError(
                f"VCF line {line_number} has {len(line_columns)} columns, expected at least 8"
            )

        chromosome_name, position_text, _, reference_allele, alternate_allele, _, filter_value, info_text = (
            line_columns[:8]
        )
        info_fields = self._parse_info_fields(info_text)
        try:
            variant_position = int(position_text)
            read_depth = int(info_fields.get("DP", 0))
            allele_fraction = float(info_fields.get("VAF", info_fields.get("AF", 0)))
        except ValueError as e:
            raise QuarantineError(f"VCF line {line_number} has a non-numeric value: {e}") from e

        return {
            "case_id": case_id,
            "gene": info_fields.get("GENE", "").strip().upper(),
            "chromosome": chromosome_name,
            "position": variant_position,
            "reference_allele": reference_allele,
            "alternate_allele": alternate_allele,
            "filter_status": filter_value,
            "read_depth": read_depth,
            "allele_fraction": allele_fraction,
        }

    def _variant_passes_filters(self, variant_row: dict) -> bool:
        return (
            variant_row["filter_status"] == PASSING_FILTER
            and variant_row["gene"] != ""
            and variant_row["read_depth"] >= self.min_variant_depth
            and variant_row["allele_fraction"] >= self.min_variant_vaf
        )

    @staticmethod
    def _parse_info_fields(info_text: str) -> dict:
        info_fields = {}
        for info_entry in info_text.split(";"):
            if "=" in info_entry:
                field_name, field_value = info_entry.split("=", 1)
                info_fields[field_name] = field_value
        return info_fields

    @staticmethod
    def _check_header_columns(header_line: str) -> None:
        header_columns = header_line.split("\t")[: len(VCF_FIXED_COLUMNS)]
        if header_columns != VCF_FIXED_COLUMNS:
            raise QuarantineError(f"VCF header columns are wrong: {header_columns}")

    @staticmethod
    def _open_text_stream(vcf_stream: BinaryIO, file_extension: str) -> io.TextIOWrapper:
        if file_extension == ".vcf.gz":
            vcf_stream = gzip.GzipFile(fileobj=vcf_stream)
        return io.TextIOWrapper(vcf_stream, encoding="utf-8", errors="strict")
