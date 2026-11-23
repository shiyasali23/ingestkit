import gzip
import io

import pytest

from ingestkit.genomics_reader import GenomicsReader
from ingestkit.pipeline_errors import QuarantineError
from tests.conftest import build_vcf_text

GOOD_VARIANT = "chr2\t197402110\t.\tT\tC\t60\tPASS\tGENE=SF3B1;VAF=0.38;DP=850"
LOW_QUALITY_VARIANT = "chr17\t7674220\t.\tG\tA\t18\tLowQual\tGENE=TP53;VAF=0.02;DP=90"
LOW_DEPTH_VARIANT = "chr4\t105275123\t.\tC\tT\t55\tPASS\tGENE=tet2;VAF=0.12;DP=40"


def read_vcf(vcf_bytes: bytes, file_extension: str = ".vcf"):
    genomics_reader = GenomicsReader(min_variant_depth=100, min_variant_vaf=0.05)
    return genomics_reader.read_variants(io.BytesIO(vcf_bytes), file_extension, case_id="C-0042")


def test_passing_variant_is_kept_with_all_fields():
    genomics_result = read_vcf(build_vcf_text([GOOD_VARIANT]))
    assert genomics_result.kept_variant_count == 1
    assert genomics_result.variant_rows[0] == {
        "case_id": "C-0042",
        "gene": "SF3B1",
        "chromosome": "chr2",
        "position": 197402110,
        "reference_allele": "T",
        "alternate_allele": "C",
        "filter_status": "PASS",
        "read_depth": 850,
        "allele_fraction": 0.38,
    }


def test_low_quality_and_low_depth_variants_are_dropped():
    genomics_result = read_vcf(build_vcf_text([GOOD_VARIANT, LOW_QUALITY_VARIANT, LOW_DEPTH_VARIANT]))
    assert genomics_result.total_variant_count == 3
    assert [row["gene"] for row in genomics_result.variant_rows] == ["SF3B1"]


def test_vcf_without_variants_is_valid():
    genomics_result = read_vcf(build_vcf_text([]))
    assert genomics_result.total_variant_count == 0
    assert genomics_result.variant_rows == []


def test_gzipped_vcf_is_read():
    genomics_result = read_vcf(gzip.compress(build_vcf_text([GOOD_VARIANT])), file_extension=".vcf.gz")
    assert genomics_result.kept_variant_count == 1


def test_file_without_vcf_header_is_quarantined():
    with pytest.raises(QuarantineError, match="Not a VCF file"):
        read_vcf(b"gene,position\nSF3B1,123\n")


def test_short_variant_line_is_quarantined():
    with pytest.raises(QuarantineError, match="line 3"):
        read_vcf(build_vcf_text(["chr2\t197402110\t.\tT"]))


def test_non_numeric_position_is_quarantined():
    with pytest.raises(QuarantineError, match="non-numeric"):
        read_vcf(build_vcf_text(["chr2\tabc\t.\tT\tC\t60\tPASS\tGENE=SF3B1;VAF=0.38;DP=850"]))
