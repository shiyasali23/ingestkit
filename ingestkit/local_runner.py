import multiprocessing
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait

from ingestkit.batch_planner import TransferBatch
from ingestkit.pipeline_logging import get_pipeline_logger
from ingestkit.transfer_worker import BatchResult, TransferWorker

module_logger = get_pipeline_logger(__name__)


class LocalRunner:
    def __init__(self, worker_count: int):
        self.worker_count = worker_count

    def run_batches(
        self,
        transfer_worker: TransferWorker,
        transfer_batches: list[TransferBatch],
        on_batch_started: Callable[[str], None],
        on_batch_finished: Callable[[BatchResult], None],
        on_batch_crashed: Callable[[str, str], None],
    ) -> None:
        if not transfer_batches:
            return

        worker_count = max(1, min(self.worker_count, len(transfer_batches)))
        module_logger.info(f"Running {len(transfer_batches)} batches with {worker_count} workers")

        if worker_count == 1:
            self._run_in_current_process(
                transfer_worker, transfer_batches, on_batch_started, on_batch_finished
            )
            return

        self._run_in_process_pool(
            transfer_worker,
            transfer_batches,
            worker_count,
            on_batch_started,
            on_batch_finished,
            on_batch_crashed,
        )

    @staticmethod
    def _run_in_current_process(
        transfer_worker, transfer_batches, on_batch_started, on_batch_finished
    ) -> None:
        for transfer_batch in transfer_batches:
            on_batch_started(transfer_batch.batch_id)
            on_batch_finished(transfer_worker.run_batch(transfer_batch))

    @staticmethod
    def _run_in_process_pool(
        transfer_worker,
        transfer_batches,
        worker_count,
        on_batch_started,
        on_batch_finished,
        on_batch_crashed,
    ) -> None:
        waiting_batches = list(transfer_batches)
        running_futures: dict[Future, str] = {}

        # spawn, not fork: pyarrow and boto3 start threads, and forking a threaded process can deadlock
        spawn_context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=worker_count, mp_context=spawn_context) as process_pool:
            try:
                while waiting_batches or running_futures:
                    while waiting_batches and len(running_futures) < worker_count:
                        next_batch = waiting_batches.pop(0)
                        on_batch_started(next_batch.batch_id)
                        batch_future = process_pool.submit(transfer_worker.run_batch, next_batch)
                        running_futures[batch_future] = next_batch.batch_id

                    finished_futures, _ = wait(running_futures, return_when=FIRST_COMPLETED)
                    for finished_future in finished_futures:
                        batch_id = running_futures.pop(finished_future)
                        try:
                            batch_result = finished_future.result()
                        except Exception as e:
                            module_logger.error(f"Batch process crashed: {e}", extra={"batch_id": batch_id})
                            on_batch_crashed(batch_id, str(e))
                            continue
                        on_batch_finished(batch_result)
            except KeyboardInterrupt:
                process_pool.shutdown(wait=False, cancel_futures=True)
                raise
