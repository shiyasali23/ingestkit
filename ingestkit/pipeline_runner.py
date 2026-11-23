import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import psutil

from ingestkit.batch_planner import BatchPlanner, TransferBatch
from ingestkit.case_linker import CaseLinker
from ingestkit.clinical_reader import ClinicalReader
from ingestkit.file_inventory import FileInventory
from ingestkit.file_transformer import FileTransformer
from ingestkit.genomics_reader import GenomicsReader
from ingestkit.kubernetes_runner import KubernetesRunner
from ingestkit.local_runner import LocalRunner
from ingestkit.pipeline_config import PipelineConfig
from ingestkit.pipeline_errors import FatalError
from ingestkit.pipeline_logging import get_pipeline_logger
from ingestkit.privacy_filter import PrivacyFilter
from ingestkit.slide_reader import SlideReader
from ingestkit.state_schema import RUN_COMPLETED, RUN_COMPLETED_WITH_FAILURES, RUN_FAILED, RUN_INTERRUPTED
from ingestkit.state_store import StateStore
from ingestkit.storage_factory import create_storage
from ingestkit.table_writer import TableWriter
from ingestkit.transfer_worker import TransferWorker, build_staging_folder

module_logger = get_pipeline_logger(__name__)


@dataclass
class RunPlan:
    planned_batches: list[TransferBatch]
    skipped_file_count: int
    parallel_slots: int

    @property
    def planned_file_count(self) -> int:
        return sum(transfer_batch.file_count for transfer_batch in self.planned_batches)

    @property
    def planned_bytes(self) -> int:
        return sum(transfer_batch.total_bytes for transfer_batch in self.planned_batches)


class PipelineRunner:
    def __init__(
        self, pipeline_config: PipelineConfig, state_store: StateStore | None = None, log_level: str = "INFO"
    ):
        self.pipeline_config = pipeline_config
        self.log_level = log_level
        self.source_storage = create_storage(pipeline_config.source_path, pipeline_config.s3_endpoint_url)
        self.destination_storage = create_storage(
            pipeline_config.destination_path, pipeline_config.s3_endpoint_url
        )
        self.state_store = state_store or StateStore(pipeline_config.state_database_url)
        self.state_store.create_tables()

        self.file_inventory = FileInventory(pipeline_config.case_id_pattern)
        self.batch_planner = BatchPlanner(pipeline_config.batch_size_bytes)
        self.table_writer = TableWriter(self.destination_storage, pipeline_config.chunk_size_bytes)
        self.case_linker = CaseLinker(self.table_writer, pipeline_config.gene_panel)

    @property
    def source_location(self) -> str:
        return self.source_storage.describe_location("")

    @property
    def destination_location(self) -> str:
        return self.destination_storage.describe_location("")

    def plan_run(self) -> RunPlan:
        manifest_entries = self.file_inventory.build_manifest(self.source_storage)
        finished_source_keys = self.state_store.load_finished_source_keys(self.source_location)
        new_entries = [
            manifest_entry
            for manifest_entry in manifest_entries
            if (manifest_entry.relative_path, manifest_entry.size_bytes) not in finished_source_keys
        ]
        parallel_slots = self.count_parallel_slots()
        return RunPlan(
            planned_batches=self.batch_planner.plan_batches(new_entries, parallel_slots),
            skipped_file_count=len(manifest_entries) - len(new_entries),
            parallel_slots=parallel_slots,
        )

    def count_parallel_slots(self) -> int:
        if self.pipeline_config.runner_type == "kubernetes":
            return self.pipeline_config.max_parallel_jobs
        logical_cores = psutil.cpu_count(logical=True) or 1
        return max(1, min(logical_cores, self.pipeline_config.max_workers))

    def create_run(self) -> str:
        run_plan = self.plan_run()
        run_id = self._create_run_id()
        self.state_store.create_run(
            run_id=run_id,
            runner_type=self.pipeline_config.runner_type,
            source_location=self.source_location,
            destination_location=self.destination_location,
            code_version=self._read_code_version(),
            config_snapshot=self.pipeline_config.safe_snapshot(),
        )
        self.state_store.save_planned_batches(run_id, run_plan.planned_batches)
        module_logger.info(
            f"Planned {run_plan.planned_file_count} files in {len(run_plan.planned_batches)} batches, "
            f"skipped {run_plan.skipped_file_count} already handled",
            extra={"run_id": run_id},
        )
        return run_id

    def start_run(self) -> str:
        run_id = self.create_run()
        self.execute_run(run_id)
        self.complete_run(run_id)
        return run_id

    def resume_run(self, run_id: str) -> str:
        if not self.state_store.get_run(run_id):
            raise FatalError(f"Run not found: {run_id}")
        self.destination_storage.delete_folder(build_staging_folder(run_id))
        self.state_store.reopen_run(run_id)
        self.execute_run(run_id)
        self.complete_run(run_id)
        return run_id

    def execute_run(self, run_id: str) -> None:
        try:
            if self.pipeline_config.runner_type == "kubernetes":
                self._execute_on_kubernetes(run_id)
            else:
                self._execute_locally(run_id)
        except KeyboardInterrupt:
            self.state_store.finish_run(run_id, RUN_INTERRUPTED)
            raise
        except Exception:
            self.state_store.finish_run(run_id, RUN_FAILED)
            raise

    def run_single_batch(self, run_id: str, batch_id: str) -> bool:
        transfer_batch = self.state_store.load_batch(run_id, batch_id)
        if transfer_batch is None:
            module_logger.info("Batch has nothing left to do", extra={"run_id": run_id, "batch_id": batch_id})
            return True

        self.state_store.mark_batch_running(run_id, batch_id)
        batch_result = self.build_transfer_worker(run_id).run_batch(transfer_batch)
        self.state_store.save_batch_result(run_id, batch_result)
        return batch_result.batch_succeeded

    def complete_run(self, run_id: str) -> dict:
        case_rows = self.case_linker.build_case_rows(
            self.state_store.load_ingested_files(self.destination_location)
        )
        self.case_linker.write_case_table(case_rows)
        self.destination_storage.delete_folder(build_staging_folder(run_id))

        unfinished_batch_count = self.state_store.count_unfinished_batches(run_id)
        final_status = RUN_COMPLETED_WITH_FAILURES if unfinished_batch_count else RUN_COMPLETED
        self.state_store.finish_run(run_id, final_status)
        self.state_store.record_audit_event(run_id, "cases_linked", {"case_count": len(case_rows)})
        return {"run_id": run_id, "run_status": final_status, **self.state_store.summarize_run(run_id)}

    def build_transfer_worker(self, run_id: str) -> TransferWorker:
        file_transformer = FileTransformer(
            destination_storage=self.destination_storage,
            table_writer=self.table_writer,
            slide_reader=SlideReader(),
            genomics_reader=GenomicsReader(
                min_variant_depth=self.pipeline_config.min_variant_depth,
                min_variant_vaf=self.pipeline_config.min_variant_vaf,
            ),
            clinical_reader=ClinicalReader(self.pipeline_config.clinical_required_fields),
            privacy_filter=PrivacyFilter(
                self.pipeline_config.pseudonym_salt,
                self.pipeline_config.clinical_identifier_fields,
            ),
        )
        return TransferWorker(
            source_storage=self.source_storage,
            destination_storage=self.destination_storage,
            file_transformer=file_transformer,
            retry_limit=self.pipeline_config.retry_limit,
            retry_delay_seconds=self.pipeline_config.retry_delay_seconds,
            chunk_size_bytes=self.pipeline_config.chunk_size_bytes,
            run_id=run_id,
            log_level=self.log_level,
        )

    def _execute_locally(self, run_id: str) -> None:
        local_runner = LocalRunner(self.count_parallel_slots())
        local_runner.run_batches(
            transfer_worker=self.build_transfer_worker(run_id),
            transfer_batches=self.state_store.load_resumable_batches(run_id),
            on_batch_started=lambda batch_id: self.state_store.mark_batch_running(run_id, batch_id),
            on_batch_finished=lambda batch_result: self.state_store.save_batch_result(run_id, batch_result),
            on_batch_crashed=lambda batch_id, error_message: self.state_store.mark_batch_crashed(
                run_id, batch_id, error_message
            ),
        )

    def _execute_on_kubernetes(self, run_id: str) -> None:
        kubernetes_runner = KubernetesRunner(
            namespace=self.pipeline_config.kubernetes_namespace,
            job_template_path=self.pipeline_config.kubernetes_job_template,
            worker_image=self.pipeline_config.worker_image,
            max_parallel_jobs=self.pipeline_config.max_parallel_jobs,
        )
        kubernetes_runner.run_batches(
            run_id=run_id,
            batch_ids=self.state_store.list_resumable_batch_ids(run_id),
            on_batch_crashed=lambda batch_id, error_message: self.state_store.mark_batch_crashed(
                run_id, batch_id, error_message
            ),
        )

    @staticmethod
    def _create_run_id() -> str:
        run_timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        return f"run-{run_timestamp}-{uuid.uuid4().hex[:6]}"

    @staticmethod
    def _read_code_version() -> str:
        try:
            git_result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
            )
            return git_result.stdout.strip() or "unknown"
        except (OSError, subprocess.SubprocessError):
            return "unknown"
