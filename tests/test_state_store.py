import json

from sqlalchemy import select

from ingestkit.batch_planner import TransferBatch
from ingestkit.file_inventory import ManifestEntry
from ingestkit.state_schema import (
    BATCH_DONE,
    BATCH_FAILED,
    FILE_DONE,
    FILE_FAILED,
    audit_events,
    transfer_batches,
)
from ingestkit.transfer_worker import BatchResult, FileResult


def build_entry(file_name: str) -> ManifestEntry:
    return ManifestEntry(
        relative_path=file_name,
        size_bytes=100,
        file_type="slide",
        file_extension=".svs",
        case_id=file_name.split(".")[0],
    )


def create_planned_run(state_store, run_id: str = "run-001") -> list[TransferBatch]:
    planned_batches = [
        TransferBatch(
            batch_id="batch-00001", manifest_entries=[build_entry("C-0001.svs"), build_entry("C-0002.svs")]
        ),
        TransferBatch(batch_id="batch-00002", manifest_entries=[build_entry("C-0003.svs")]),
    ]
    state_store.create_run(
        run_id=run_id,
        runner_type="local",
        source_location="/data/lab_upload",
        destination_location="/data/destination",
        code_version="abc123",
        config_snapshot={"batch_size_gb": 4},
    )
    state_store.save_planned_batches(run_id, planned_batches)
    return planned_batches


def build_done_result(manifest_entry: ManifestEntry) -> FileResult:
    return FileResult(
        manifest_entry=manifest_entry,
        file_status=FILE_DONE,
        source_checksum="a" * 64,
        destination_path=f"raw/slide/{'a' * 64}.svs",
    )


def read_batch_row(state_store, batch_id: str) -> dict:
    with state_store.database_engine.connect() as database_connection:
        return dict(
            database_connection.execute(
                select(transfer_batches).where(transfer_batches.c.batch_id == batch_id)
            )
            .mappings()
            .first()
        )


def test_new_run_has_all_batches_resumable(state_store):
    create_planned_run(state_store)
    resumable_batches = state_store.load_resumable_batches("run-001")
    assert [batch.batch_id for batch in resumable_batches] == ["batch-00001", "batch-00002"]
    assert resumable_batches[0].file_count == 2


def test_running_batch_counts_attempts(state_store):
    create_planned_run(state_store)
    state_store.mark_batch_running("run-001", "batch-00001")
    state_store.mark_batch_running("run-001", "batch-00001")
    assert read_batch_row(state_store, "batch-00001")["attempt_count"] == 2


def test_successful_batch_writes_lineage_and_is_not_resumable(state_store):
    planned_batches = create_planned_run(state_store)
    first_batch = planned_batches[0]
    batch_result = BatchResult(
        batch_id=first_batch.batch_id,
        file_results=[build_done_result(entry) for entry in first_batch.manifest_entries],
    )
    state_store.save_batch_result("run-001", batch_result)

    assert read_batch_row(state_store, "batch-00001")["batch_status"] == BATCH_DONE
    assert [batch.batch_id for batch in state_store.load_resumable_batches("run-001")] == ["batch-00002"]
    trace_rows = state_store.trace_destination_file(f"raw/slide/{'a' * 64}.svs")
    assert {row["source_path"] for row in trace_rows} == {"C-0001.svs", "C-0002.svs"}
    assert trace_rows[0]["code_version"] == "abc123"


def test_failed_batch_resumes_only_unfinished_files(state_store):
    planned_batches = create_planned_run(state_store)
    first_entry, second_entry = planned_batches[0].manifest_entries
    batch_result = BatchResult(
        batch_id="batch-00001",
        file_results=[
            build_done_result(first_entry),
            FileResult(manifest_entry=second_entry, file_status=FILE_FAILED, status_reason="timeout"),
        ],
    )
    state_store.save_batch_result("run-001", batch_result)

    assert read_batch_row(state_store, "batch-00001")["batch_status"] == BATCH_FAILED
    resumable_batches = state_store.load_resumable_batches("run-001")
    resumed_first_batch = next(batch for batch in resumable_batches if batch.batch_id == "batch-00001")
    assert [entry.relative_path for entry in resumed_first_batch.manifest_entries] == ["C-0002.svs"]


def test_completed_files_are_found_for_same_source(state_store):
    planned_batches = create_planned_run(state_store)
    first_batch = planned_batches[0]
    state_store.save_batch_result(
        "run-001",
        BatchResult(
            batch_id=first_batch.batch_id,
            file_results=[build_done_result(entry) for entry in first_batch.manifest_entries],
        ),
    )
    completed_keys = state_store.load_finished_source_keys("/data/lab_upload")
    assert completed_keys == {("C-0001.svs", 100), ("C-0002.svs", 100)}
    assert state_store.load_finished_source_keys("/other/source") == set()


def test_audit_trail_records_every_step(state_store):
    planned_batches = create_planned_run(state_store)
    state_store.mark_batch_running("run-001", "batch-00001")
    state_store.save_batch_result(
        "run-001",
        BatchResult(
            batch_id="batch-00001",
            file_results=[build_done_result(entry) for entry in planned_batches[0].manifest_entries],
        ),
    )
    state_store.finish_run("run-001", "completed")

    with state_store.database_engine.connect() as database_connection:
        event_rows = database_connection.execute(
            select(audit_events.c.event_type, audit_events.c.event_detail).order_by(audit_events.c.event_id)
        ).all()
    event_types = [event_type for event_type, _ in event_rows]
    assert event_types == [
        "run_started",
        "run_planned",
        "batch_started",
        "file_done",
        "file_done",
        "batch_done",
        "run_finished",
    ]
    assert json.loads(event_rows[3][1])["source_path"] == "C-0001.svs"


def test_run_summary_counts_statuses(state_store):
    create_planned_run(state_store)
    run_summary = state_store.summarize_run("run-001")
    assert run_summary["batches_by_status"] == {"pending": 2}
    assert run_summary["files_by_status"] == {"pending": 3}
    assert state_store.find_latest_run_id() == "run-001"
