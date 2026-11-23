from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestkit.kubernetes_runner import KubernetesRunner, build_job_name
from ingestkit.pipeline_errors import FatalError

JOB_TEMPLATE_PATH = str(Path(__file__).parent.parent / "kubernetes" / "worker_job.yaml")


class FakeBatchApi:
    def __init__(self, final_conditions: dict[str, str]):
        self.final_conditions = final_conditions
        self.created_jobs: list[dict] = []
        self.status_reads: dict[str, int] = {}
        self.active_job_names: set[str] = set()
        self.most_active_jobs = 0

    def create_namespaced_job(self, namespace: str, body: dict):
        self.created_jobs.append(body)
        self.active_job_names.add(body["metadata"]["name"])
        self.most_active_jobs = max(self.most_active_jobs, len(self.active_job_names))

    def read_namespaced_job_status(self, name: str, namespace: str):
        self.status_reads[name] = self.status_reads.get(name, 0) + 1
        if self.status_reads[name] < 2:
            return SimpleNamespace(status=SimpleNamespace(conditions=None))
        batch_id = name.rsplit("-batch-", 1)[1]
        condition_type = self.final_conditions.get(f"batch-{batch_id}", "Complete")
        self.active_job_names.discard(name)
        job_condition = SimpleNamespace(type=condition_type, status="True")
        return SimpleNamespace(status=SimpleNamespace(conditions=[job_condition]))


def build_runner(fake_api: FakeBatchApi, max_parallel_jobs: int = 2) -> KubernetesRunner:
    return KubernetesRunner(
        namespace="ingestkit",
        job_template_path=JOB_TEMPLATE_PATH,
        worker_image="registry.example/ingestkit-worker:1.0",
        max_parallel_jobs=max_parallel_jobs,
        poll_seconds=0,
        batch_api=fake_api,
    )


def test_job_manifest_runs_worker_for_one_batch():
    job_manifest = build_runner(FakeBatchApi({})).build_job_manifest(
        "run-20261122-101500-ab12cd", "batch-00007"
    )

    worker_container = job_manifest["spec"]["template"]["spec"]["containers"][0]
    assert job_manifest["metadata"]["name"] == "ingestkit-run-20261122-101500-ab12cd-batch-00007"
    assert worker_container["image"] == "registry.example/ingestkit-worker:1.0"
    assert worker_container["args"][:5] == [
        "worker",
        "--run-id",
        "run-20261122-101500-ab12cd",
        "--batch-id",
        "batch-00007",
    ]
    assert worker_container["resources"]["limits"]["memory"] == "4Gi"


def test_failed_job_is_reported_as_crashed_batch():
    fake_api = FakeBatchApi({"batch-00002": "Failed"})
    crashed_batches = []

    build_runner(fake_api).run_batches(
        run_id="run-1",
        batch_ids=["batch-00001", "batch-00002", "batch-00003"],
        on_batch_crashed=lambda batch_id, error_message: crashed_batches.append(batch_id),
    )

    assert len(fake_api.created_jobs) == 3
    assert crashed_batches == ["batch-00002"]


def test_parallel_job_limit_is_respected():
    fake_api = FakeBatchApi({})
    build_runner(fake_api, max_parallel_jobs=2).run_batches(
        run_id="run-1",
        batch_ids=[f"batch-{batch_number:05d}" for batch_number in range(1, 7)],
        on_batch_crashed=lambda *_: None,
    )
    assert len(fake_api.created_jobs) == 6
    assert fake_api.most_active_jobs == 2


def test_job_name_is_valid_for_kubernetes():
    job_name = build_job_name("RUN_2026 11 22", "Batch-00001" * 10)
    assert len(job_name) <= 63
    assert job_name == job_name.lower()
    assert not job_name.endswith("-")


def test_api_error_stops_the_run():
    class BrokenBatchApi(FakeBatchApi):
        def create_namespaced_job(self, namespace, body):
            raise RuntimeError("cluster unreachable")

    with pytest.raises(FatalError, match="cluster unreachable"):
        build_runner(BrokenBatchApi({})).run_batches(
            "run-1", ["batch-00001"], on_batch_crashed=lambda *_: None
        )
