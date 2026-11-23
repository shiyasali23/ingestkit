import os
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task

from ingestkit.pipeline_config import load_pipeline_config
from ingestkit.pipeline_runner import PipelineRunner

CONFIG_PATH = os.getenv("INGESTKIT_CONFIG_PATH", "/opt/ingestkit/pipeline_config.yaml")


def build_pipeline_runner() -> PipelineRunner:
    return PipelineRunner(load_pipeline_config(CONFIG_PATH))


@dag(
    dag_id="ingestkit_transfer_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestkit", "ingestion"],
)
def ingestkit_transfer_pipeline():
    @task
    def create_run() -> str:
        return build_pipeline_runner().create_run()

    @task
    def list_batches(run_id: str) -> list[dict]:
        batch_ids = build_pipeline_runner().state_store.list_resumable_batch_ids(run_id)
        return [{"run_id": run_id, "batch_id": batch_id} for batch_id in batch_ids]

    # Each batch is its own task, so Airflow retries only the batch that failed. On EKS the same
    # call runs as `ingestkit worker` inside a KubernetesPodOperator with the worker image.
    @task(max_active_tis_per_dag=8)
    def transfer_batch(batch_reference: dict) -> None:
        batch_succeeded = build_pipeline_runner().run_single_batch(
            run_id=batch_reference["run_id"],
            batch_id=batch_reference["batch_id"],
        )
        if not batch_succeeded:
            raise AirflowFailException(f"Batch {batch_reference['batch_id']} has failed files")

    @task(trigger_rule="all_done")
    def complete_run(run_id: str) -> dict:
        return build_pipeline_runner().complete_run(run_id)

    run_id = create_run()
    batch_references = list_batches(run_id)
    transfer_tasks = transfer_batch.expand(batch_reference=batch_references)
    transfer_tasks >> complete_run(run_id)


ingestkit_transfer_pipeline()
