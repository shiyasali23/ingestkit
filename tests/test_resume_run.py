from sqlalchemy import select

from ingestkit.pipeline_errors import RetryableError
from ingestkit.pipeline_runner import PipelineRunner
from ingestkit.state_schema import RUN_COMPLETED, RUN_COMPLETED_WITH_FAILURES, transfer_batches
from ingestkit.transfer_worker import TransferWorker


def read_attempt_counts(state_store, run_id: str) -> dict:
    with state_store.database_engine.connect() as database_connection:
        batch_rows = database_connection.execute(
            select(transfer_batches.c.batch_id, transfer_batches.c.attempt_count).where(
                transfer_batches.c.run_id == run_id
            )
        ).all()
    return {batch_id: attempt_count for batch_id, attempt_count in batch_rows}


def list_raw_files(destination_folder) -> list:
    return sorted(path.name for path in (destination_folder / "raw").rglob("*") if path.is_file())


def test_resume_reruns_only_failed_batch(pipeline_config, state_store, destination_folder, monkeypatch):
    original_copy_method = TransferWorker._process_file

    def failing_copy_method(transfer_worker, manifest_entry):
        if manifest_entry.relative_path == "C-0002.svs":
            raise RetryableError("Simulated network outage")
        return original_copy_method(transfer_worker, manifest_entry)

    monkeypatch.setattr(TransferWorker, "_process_file", failing_copy_method)
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    run_id = pipeline_runner.start_run()

    assert state_store.get_run(run_id)["run_status"] == RUN_COMPLETED_WITH_FAILURES
    first_attempt_counts = read_attempt_counts(state_store, run_id)
    files_after_first_run = list_raw_files(destination_folder)
    assert len(files_after_first_run) == 5

    monkeypatch.setattr(TransferWorker, "_process_file", original_copy_method)
    pipeline_runner.resume_run(run_id)

    assert state_store.get_run(run_id)["run_status"] == RUN_COMPLETED
    second_attempt_counts = read_attempt_counts(state_store, run_id)
    rerun_batches = [
        batch_id
        for batch_id, attempt_count in second_attempt_counts.items()
        if attempt_count > first_attempt_counts[batch_id]
    ]
    assert len(rerun_batches) == 1
    assert len(list_raw_files(destination_folder)) == 6
    assert state_store.summarize_run(run_id)["files_by_status"] == {"done": 6}


def test_resume_of_finished_run_does_nothing(pipeline_config, state_store):
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    run_id = pipeline_runner.start_run()
    attempt_counts_before = read_attempt_counts(state_store, run_id)

    pipeline_runner.resume_run(run_id)

    assert read_attempt_counts(state_store, run_id) == attempt_counts_before
    assert state_store.get_run(run_id)["run_status"] == RUN_COMPLETED


def test_resume_removes_half_written_files(pipeline_config, state_store, destination_folder):
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    run_id = pipeline_runner.start_run()
    leftover_file = destination_folder / "staging" / run_id / "crashed_copy.svs.partial"
    leftover_file.parent.mkdir(parents=True)
    leftover_file.write_bytes(b"half written")

    pipeline_runner.resume_run(run_id)

    assert not leftover_file.exists()
