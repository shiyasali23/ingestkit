import copy
import re
import time
from collections.abc import Callable
from pathlib import Path

import yaml

from ingestkit.pipeline_errors import ConfigError, FatalError
from ingestkit.pipeline_logging import get_pipeline_logger

MAX_JOB_NAME_LENGTH = 63
ALREADY_EXISTS_STATUS = 409
WORKER_CONFIG_PATH = "/etc/ingestkit/pipeline_config.yaml"

module_logger = get_pipeline_logger(__name__)


def build_job_name(run_id: str, batch_id: str) -> str:
    job_name = re.sub(r"[^a-z0-9-]", "-", f"ingestkit-{run_id}-{batch_id}".lower())
    return job_name[:MAX_JOB_NAME_LENGTH].rstrip("-")


class KubernetesRunner:
    def __init__(
        self,
        namespace: str,
        job_template_path: str,
        worker_image: str,
        max_parallel_jobs: int,
        poll_seconds: float = 10,
        batch_api=None,
    ):
        self.namespace = namespace
        self.worker_image = worker_image
        self.max_parallel_jobs = max_parallel_jobs
        self.poll_seconds = poll_seconds
        self.job_template = self._load_job_template(job_template_path)
        self.batch_api = batch_api or self._create_batch_api()

    def build_job_manifest(self, run_id: str, batch_id: str) -> dict:
        job_manifest = copy.deepcopy(self.job_template)
        job_manifest["metadata"]["name"] = build_job_name(run_id, batch_id)
        job_manifest["metadata"].setdefault("labels", {}).update(
            {"ingestkit/run-id": run_id, "ingestkit/batch-id": batch_id}
        )

        worker_container = job_manifest["spec"]["template"]["spec"]["containers"][0]
        worker_container["image"] = self.worker_image
        worker_container["args"] = [
            "worker",
            "--run-id",
            run_id,
            "--batch-id",
            batch_id,
            "--config",
            WORKER_CONFIG_PATH,
        ]
        return job_manifest

    def run_batches(
        self, run_id: str, batch_ids: list[str], on_batch_crashed: Callable[[str, str], None]
    ) -> None:
        waiting_batch_ids = list(batch_ids)
        running_jobs: dict[str, str] = {}
        module_logger.info(
            f"Submitting {len(batch_ids)} batches as Kubernetes Jobs", extra={"run_id": run_id}
        )

        while waiting_batch_ids or running_jobs:
            while waiting_batch_ids and len(running_jobs) < self.max_parallel_jobs:
                next_batch_id = waiting_batch_ids.pop(0)
                running_jobs[self._submit_job(run_id, next_batch_id)] = next_batch_id

            for job_name, batch_id in list(running_jobs.items()):
                job_outcome = self._read_job_outcome(job_name)
                if job_outcome == "running":
                    continue
                running_jobs.pop(job_name)
                if job_outcome == "failed":
                    # The pod writes its own result to the state database; a failed Job means
                    # the pod crashed before it could, so the batch is marked here instead.
                    on_batch_crashed(batch_id, f"Kubernetes Job {job_name} failed")

            if running_jobs:
                time.sleep(self.poll_seconds)

    def _submit_job(self, run_id: str, batch_id: str) -> str:
        job_manifest = self.build_job_manifest(run_id, batch_id)
        job_name = job_manifest["metadata"]["name"]
        try:
            self.batch_api.create_namespaced_job(namespace=self.namespace, body=job_manifest)
        except Exception as e:
            if getattr(e, "status", None) != ALREADY_EXISTS_STATUS:
                raise FatalError(f"Error creating Kubernetes Job {job_name}: {e}") from e
            module_logger.warning(
                f"Job {job_name} already exists, waiting for it", extra={"batch_id": batch_id}
            )
        return job_name

    def _read_job_outcome(self, job_name: str) -> str:
        try:
            job_status = self.batch_api.read_namespaced_job_status(
                name=job_name, namespace=self.namespace
            ).status
        except Exception as e:
            module_logger.warning(f"Error reading Job {job_name}, will retry: {e}")
            return "running"

        for job_condition in job_status.conditions or []:
            if job_condition.status != "True":
                continue
            if job_condition.type == "Complete":
                return "succeeded"
            if job_condition.type == "Failed":
                return "failed"
        return "running"

    @staticmethod
    def _load_job_template(job_template_path: str) -> dict:
        template_file = Path(job_template_path)
        if not template_file.is_file():
            raise ConfigError(f"Kubernetes Job template not found: {template_file}")
        return yaml.safe_load(template_file.read_text())

    @staticmethod
    def _create_batch_api():
        try:
            from kubernetes import client, config
        except ImportError as e:
            raise ConfigError("Install the kubernetes package to use runner_type: kubernetes") from e

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        return client.BatchV1Api()
