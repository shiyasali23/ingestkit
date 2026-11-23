import json
from pathlib import Path

import numpy
import pytest
import tifffile

from ingestkit.pipeline_config import PipelineConfig

TEST_SALT = "test-salt-not-secret"


def write_sample_file(folder_path: Path, file_name: str, file_content: bytes) -> Path:
    file_path = folder_path / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(file_content)
    return file_path


def write_sample_slide(slide_path: Path, random_seed: int, edge_pixels: int = 256) -> Path:
    slide_path.parent.mkdir(parents=True, exist_ok=True)
    random_generator = numpy.random.default_rng(random_seed)
    slide_pixels = random_generator.integers(0, 255, size=(edge_pixels, edge_pixels, 3), dtype=numpy.uint8)
    tifffile.imwrite(slide_path, slide_pixels, tile=(64, 64), photometric="rgb")
    return slide_path


def build_vcf_text(variant_lines: list[str]) -> bytes:
    header_lines = [
        "##fileformat=VCFv4.2",
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
    ]
    return ("\n".join(header_lines + variant_lines) + "\n").encode("utf-8")


def build_clinical_json(case_id: str, **extra_fields) -> bytes:
    clinical_record = {"case_id": case_id, "age": 71, "sex": "male", "diagnosis": "MDS"}
    clinical_record.update(extra_fields)
    return json.dumps(clinical_record).encode("utf-8")


@pytest.fixture
def source_folder(tmp_path: Path) -> Path:
    folder_path = tmp_path / "lab_upload"
    folder_path.mkdir()
    write_sample_slide(folder_path / "C-0001.svs", random_seed=1)
    write_sample_file(
        folder_path,
        "C-0001.vcf",
        build_vcf_text(["chr2\t197402110\t.\tT\tC\t60\tPASS\tGENE=SF3B1;VAF=0.38;DP=850"]),
    )
    write_sample_file(folder_path, "C-0001.json", build_clinical_json("C-0001", name="Jane Example"))
    write_sample_slide(folder_path / "C-0002.svs", random_seed=2)
    write_sample_file(
        folder_path,
        "C-0002.vcf",
        build_vcf_text(["chr17\t7674220\t.\tG\tA\t60\tPASS\tGENE=TP53;VAF=0.41;DP=600"]),
    )
    write_sample_file(folder_path, "nested/C-0003.json", build_clinical_json("C-0003", diagnosis="control"))
    return folder_path


@pytest.fixture
def destination_folder(tmp_path: Path) -> Path:
    folder_path = tmp_path / "destination"
    folder_path.mkdir()
    return folder_path


@pytest.fixture
def pipeline_config(tmp_path: Path, source_folder: Path, destination_folder: Path) -> PipelineConfig:
    return PipelineConfig(
        source_path=str(source_folder),
        destination_path=str(destination_folder),
        state_database_url=f"sqlite:///{tmp_path / 'state.db'}",
        batch_size_gb=0.0002,
        max_workers=1,
        retry_limit=2,
        retry_delay_seconds=0,
        chunk_size_mb=1,
        pseudonym_salt=TEST_SALT,
        gene_panel=["SF3B1", "TP53", "TET2"],
    )


@pytest.fixture
def state_store(pipeline_config: PipelineConfig):
    from ingestkit.state_store import StateStore

    test_state_store = StateStore(pipeline_config.state_database_url)
    test_state_store.create_tables()
    return test_state_store
