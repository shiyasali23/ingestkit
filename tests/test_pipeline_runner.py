import hashlib

from sqlalchemy import select

from ingestkit.pipeline_runner import PipelineRunner
from ingestkit.state_schema import RUN_COMPLETED, audit_events


def test_full_run_with_process_pool(
    pipeline_config, state_store, source_folder, destination_folder, monkeypatch
):
    monkeypatch.setattr(PipelineRunner, "count_parallel_slots", lambda pipeline_runner: 2)
    pipeline_runner = PipelineRunner(pipeline_config, state_store)

    run_id = pipeline_runner.start_run()

    assert state_store.get_run(run_id)["run_status"] == RUN_COMPLETED
    assert state_store.summarize_run(run_id)["files_by_status"] == {"done": 6}
    slide_checksum = hashlib.sha256((source_folder / "C-0001.svs").read_bytes()).hexdigest()
    assert (destination_folder / "raw" / "slide" / f"{slide_checksum}.svs").exists()


def test_second_run_skips_already_ingested_files(pipeline_config, state_store):
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    pipeline_runner.start_run()

    second_plan = pipeline_runner.plan_run()
    assert second_plan.planned_file_count == 0
    assert second_plan.skipped_file_count == 6


def test_changed_file_is_ingested_again(pipeline_config, state_store, source_folder):
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    pipeline_runner.start_run()
    (source_folder / "C-0001.json").write_bytes(b'{"case_id": "C-0001", "age": 71}')

    second_plan = pipeline_runner.plan_run()
    assert second_plan.planned_file_count == 1


def test_worker_count_respects_max_workers(pipeline_config, state_store):
    limited_config = pipeline_config.model_copy(update={"max_workers": 1})
    assert PipelineRunner(limited_config, state_store).count_parallel_slots() == 1


def test_unchanged_quarantined_file_is_not_planned_again(pipeline_config, state_store, source_folder):
    (source_folder / "scanner_notes.docx").write_bytes(b"unsupported")
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    run_id = pipeline_runner.start_run()

    assert state_store.summarize_run(run_id)["files_by_status"] == {"done": 6, "quarantined": 1}
    assert pipeline_runner.plan_run().planned_file_count == 0


def test_completed_run_writes_processed_tables_and_cases(pipeline_config, state_store):
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    run_id = pipeline_runner.start_run()

    case_rows = pipeline_runner.table_writer.read_table("processed/cases/cases.parquet")
    cases_by_id = {case_row["case_id"]: case_row for case_row in case_rows}
    assert cases_by_id["C-0001"]["is_complete"] is True
    assert cases_by_id["C-0001"]["SF3B1_mutated"] is True
    assert cases_by_id["C-0002"]["missing_parts"] == "clinical"
    assert cases_by_id["C-0003"]["missing_parts"] == "slide,genomics"

    with state_store.database_engine.connect() as database_connection:
        event_types = (
            database_connection.execute(
                select(audit_events.c.event_type).where(audit_events.c.run_id == run_id)
            )
            .scalars()
            .all()
        )
    assert "cases_linked" in event_types


def test_plan_splits_work_across_parallel_workers(pipeline_config, state_store, monkeypatch):
    monkeypatch.setattr(PipelineRunner, "count_parallel_slots", lambda pipeline_runner: 3)
    run_plan = PipelineRunner(pipeline_config, state_store).plan_run()

    assert run_plan.parallel_slots == 3
    assert len(run_plan.planned_batches) == 3


def test_clinical_names_never_reach_processed_tables(pipeline_config, state_store, destination_folder):
    PipelineRunner(pipeline_config, state_store).start_run()

    processed_clinical_files = list((destination_folder / "processed" / "clinical").glob("*.parquet"))
    assert processed_clinical_files
    for processed_file in processed_clinical_files:
        assert b"Jane Example" not in processed_file.read_bytes()


def test_bad_clinical_file_is_quarantined_and_not_stored_in_raw(
    pipeline_config, state_store, source_folder, destination_folder
):
    (source_folder / "C-0004.json").write_bytes(b'{"case_id": "C-0004", "age": 70}')
    run_id = PipelineRunner(pipeline_config, state_store).start_run()

    run_summary = state_store.summarize_run(run_id)
    assert run_summary["files_by_status"] == {"done": 6, "quarantined": 1}
    assert (destination_folder / "quarantine" / "C-0004.json").exists()
    assert "missing required fields" in run_summary["problem_files"][0]["reason"]


def test_trace_finds_source_of_processed_table(pipeline_config, state_store):
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    run_id = pipeline_runner.start_run()
    slide_row = next(
        row
        for row in pipeline_runner.state_store.load_ingested_files(pipeline_runner.destination_location)
        if row["file_type"] == "slide"
    )

    trace_rows = pipeline_runner.state_store.trace_destination_file(slide_row["processed_path"])
    assert trace_rows[0]["run_id"] == run_id
    assert trace_rows[0]["source_checksum"] == slide_row["source_checksum"]


def test_single_batch_mode_matches_full_run(pipeline_config, state_store):
    pipeline_runner = PipelineRunner(pipeline_config, state_store)
    run_id = pipeline_runner.create_run()

    for batch_id in state_store.list_resumable_batch_ids(run_id):
        assert pipeline_runner.run_single_batch(run_id, batch_id) is True
    pipeline_runner.complete_run(run_id)

    assert state_store.get_run(run_id)["run_status"] == RUN_COMPLETED
    assert state_store.summarize_run(run_id)["files_by_status"] == {"done": 6}
