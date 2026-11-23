import os
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator

from ingestkit.pipeline_errors import ConfigError

BYTES_PER_GB = 1024**3
BYTES_PER_MB = 1024**2
DATABASE_URL_VARIABLE = "INGESTKIT_STATE_DATABASE_URL"
PSEUDONYM_SALT_VARIABLE = "INGESTKIT_PSEUDONYM_SALT"
S3_ENDPOINT_VARIABLE = "INGESTKIT_S3_ENDPOINT_URL"


class PipelineConfig(BaseModel):
    source_path: str
    destination_path: str
    state_database_url: str = "sqlite:///ingestkit_state.db"
    s3_endpoint_url: str | None = None

    batch_size_gb: float = Field(default=4, gt=0)
    max_workers: int = Field(default=4, ge=1)
    retry_limit: int = Field(default=3, ge=0)
    retry_delay_seconds: float = Field(default=1, ge=0)
    chunk_size_mb: int = Field(default=8, ge=1)
    case_id_pattern: str = "^([^._]+)"

    pseudonym_salt: str = Field(min_length=8)
    gene_panel: list[str] = Field(
        default_factory=lambda: ["SF3B1", "TP53", "ASXL1", "TET2", "DNMT3A", "RUNX1"]
    )
    min_variant_depth: int = Field(default=100, ge=0)
    min_variant_vaf: float = Field(default=0.05, ge=0, le=1)
    clinical_required_fields: list[str] = Field(
        default_factory=lambda: ["case_id", "age", "sex", "diagnosis"]
    )
    clinical_identifier_fields: list[str] = Field(
        default_factory=lambda: ["name", "patient_name", "birth_date", "address", "phone", "email"]
    )

    runner_type: Literal["local", "kubernetes"] = "local"
    kubernetes_namespace: str = "ingestkit"
    kubernetes_job_template: str = "kubernetes/worker_job.yaml"
    worker_image: str = "ingestkit-worker:latest"
    max_parallel_jobs: int = Field(default=10, ge=1)

    @field_validator("case_id_pattern")
    @classmethod
    def check_case_id_pattern(cls, pattern_text: str) -> str:
        try:
            compiled_pattern = re.compile(pattern_text)
        except re.error as e:
            raise ValueError(f"case_id_pattern is not a valid regex: {e}") from e
        if compiled_pattern.groups < 1:
            raise ValueError("case_id_pattern needs one capture group for the case ID")
        return pattern_text

    @field_validator("gene_panel")
    @classmethod
    def normalise_gene_panel(cls, gene_names: list[str]) -> list[str]:
        return [gene_name.strip().upper() for gene_name in gene_names if gene_name.strip()]

    @property
    def batch_size_bytes(self) -> int:
        return int(self.batch_size_gb * BYTES_PER_GB)

    @property
    def chunk_size_bytes(self) -> int:
        return self.chunk_size_mb * BYTES_PER_MB

    def safe_snapshot(self) -> dict:
        return self.model_dump(exclude={"pseudonym_salt"})


def load_pipeline_config(config_path: str) -> PipelineConfig:
    config_file = Path(config_path)
    if not config_file.is_file():
        raise ConfigError(f"Config file not found: {config_file}")

    try:
        raw_settings = yaml.safe_load(config_file.read_text()) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"Config file is not valid YAML: {e}") from e

    environment_overrides = {
        "state_database_url": os.getenv(DATABASE_URL_VARIABLE),
        "pseudonym_salt": os.getenv(PSEUDONYM_SALT_VARIABLE),
        "s3_endpoint_url": os.getenv(S3_ENDPOINT_VARIABLE),
    }
    for setting_name, setting_value in environment_overrides.items():
        if setting_value:
            raw_settings[setting_name] = setting_value

    if not raw_settings.get("pseudonym_salt"):
        raise ConfigError(f"Set {PSEUDONYM_SALT_VARIABLE} (at least 8 characters) before running")

    try:
        return PipelineConfig(**raw_settings)
    except ValidationError as e:
        raise ConfigError(f"Invalid pipeline config: {e}") from e
