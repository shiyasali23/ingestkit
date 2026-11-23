import json
from datetime import UTC, datetime

from sqlalchemy import Engine, create_engine, func, insert, or_, select, update
from sqlalchemy.exc import SQLAlchemyError

from ingestkit.batch_planner import TransferBatch
from ingestkit.file_inventory import ManifestEntry
from ingestkit.pipeline_errors import FatalError
from ingestkit.state_schema import (
    BATCH_DONE,
    BATCH_FAILED,
    BATCH_PENDING,
    BATCH_RUNNING,
    FILE_DONE,
    FILE_PENDING,
    FINISHED_FILE_STATUSES,
    RESUMABLE_BATCH_STATUSES,
    RUN_RUNNING,
    audit_events,
    pipeline_runs,
    state_metadata,
    transfer_batches,
    transfer_files,
)
from ingestkit.transfer_worker import BatchResult, FileResult


def current_utc_time() -> datetime:
    return datetime.now(UTC)


class StateStore:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.database_engine: Engine = create_engine(database_url)

    def create_tables(self) -> None:
        try:
            state_metadata.create_all(self.database_engine)
        except SQLAlchemyError as e:
            raise FatalError(f"Error creating state tables: {e}") from e

    def create_run(
        self,
        run_id: str,
        runner_type: str,
        source_location: str,
        destination_location: str,
        code_version: str,
        config_snapshot: dict,
    ) -> None:
        with self.database_engine.begin() as database_connection:
            database_connection.execute(
                insert(pipeline_runs).values(
                    run_id=run_id,
                    run_status=RUN_RUNNING,
                    runner_type=runner_type,
                    source_location=source_location,
                    destination_location=destination_location,
                    code_version=code_version,
                    config_snapshot=json.dumps(config_snapshot, sort_keys=True),
                    started_at=current_utc_time(),
                )
            )
            self._insert_audit_event(database_connection, run_id, "run_started", {"runner_type": runner_type})

    def save_planned_batches(self, run_id: str, planned_batches: list[TransferBatch]) -> None:
        saved_at = current_utc_time()
        batch_rows = []
        file_rows = []
        for transfer_batch in planned_batches:
            batch_rows.append(
                {
                    "run_id": run_id,
                    "batch_id": transfer_batch.batch_id,
                    "batch_status": BATCH_PENDING,
                    "attempt_count": 0,
                    "total_bytes": transfer_batch.total_bytes,
                    "file_count": transfer_batch.file_count,
                    "updated_at": saved_at,
                }
            )
            for manifest_entry in transfer_batch.manifest_entries:
                file_rows.append(
                    self._build_pending_file_row(run_id, transfer_batch.batch_id, manifest_entry, saved_at)
                )

        planned_detail = {"batch_count": len(batch_rows), "file_count": len(file_rows)}
        with self.database_engine.begin() as database_connection:
            if batch_rows:
                database_connection.execute(insert(transfer_batches), batch_rows)
            if file_rows:
                database_connection.execute(insert(transfer_files), file_rows)
            self._insert_audit_event(database_connection, run_id, "run_planned", planned_detail)

    def mark_batch_running(self, run_id: str, batch_id: str) -> None:
        with self.database_engine.begin() as database_connection:
            database_connection.execute(
                update(transfer_batches)
                .where(transfer_batches.c.run_id == run_id, transfer_batches.c.batch_id == batch_id)
                .values(
                    batch_status=BATCH_RUNNING,
                    attempt_count=transfer_batches.c.attempt_count + 1,
                    updated_at=current_utc_time(),
                )
            )
            self._insert_audit_event(database_connection, run_id, "batch_started", {}, batch_id)

    def save_batch_result(self, run_id: str, batch_result: BatchResult) -> None:
        saved_at = current_utc_time()
        batch_status = BATCH_DONE if batch_result.batch_succeeded else BATCH_FAILED

        with self.database_engine.begin() as database_connection:
            for file_result in batch_result.file_results:
                self._save_file_result(
                    database_connection, run_id, batch_result.batch_id, file_result, saved_at
                )

            database_connection.execute(
                update(transfer_batches)
                .where(
                    transfer_batches.c.run_id == run_id, transfer_batches.c.batch_id == batch_result.batch_id
                )
                .values(batch_status=batch_status, updated_at=saved_at)
            )
            batch_detail = {
                "duration_seconds": batch_result.duration_seconds,
                "transferred_bytes": batch_result.transferred_bytes,
            }
            self._insert_audit_event(
                database_connection, run_id, f"batch_{batch_status}", batch_detail, batch_result.batch_id
            )

    def _save_file_result(
        self,
        database_connection,
        run_id: str,
        batch_id: str,
        file_result: FileResult,
        saved_at: datetime,
    ) -> None:
        source_path = file_result.manifest_entry.relative_path
        database_connection.execute(
            update(transfer_files)
            .where(transfer_files.c.run_id == run_id, transfer_files.c.source_path == source_path)
            .values(
                file_status=file_result.file_status,
                source_checksum=file_result.source_checksum,
                destination_path=file_result.destination_path,
                processed_path=file_result.processed_path,
                status_reason=file_result.status_reason,
                updated_at=saved_at,
            )
        )
        file_detail = {
            "source_path": source_path,
            "checksum": file_result.source_checksum,
            "destination_path": file_result.destination_path,
            "processed_path": file_result.processed_path,
            "reason": file_result.status_reason,
        }
        self._insert_audit_event(
            database_connection, run_id, f"file_{file_result.file_status}", file_detail, batch_id
        )

    def mark_batch_crashed(self, run_id: str, batch_id: str, error_message: str) -> None:
        with self.database_engine.begin() as database_connection:
            database_connection.execute(
                update(transfer_batches)
                .where(transfer_batches.c.run_id == run_id, transfer_batches.c.batch_id == batch_id)
                .values(batch_status=BATCH_FAILED, updated_at=current_utc_time())
            )
            self._insert_audit_event(
                database_connection, run_id, "batch_crashed", {"error": error_message}, batch_id
            )

    def finish_run(self, run_id: str, run_status: str) -> None:
        with self.database_engine.begin() as database_connection:
            database_connection.execute(
                update(pipeline_runs)
                .where(pipeline_runs.c.run_id == run_id)
                .values(run_status=run_status, finished_at=current_utc_time())
            )
            self._insert_audit_event(database_connection, run_id, "run_finished", {"run_status": run_status})

    def reopen_run(self, run_id: str) -> None:
        with self.database_engine.begin() as database_connection:
            database_connection.execute(
                update(pipeline_runs)
                .where(pipeline_runs.c.run_id == run_id)
                .values(run_status=RUN_RUNNING, finished_at=None)
            )
            self._insert_audit_event(database_connection, run_id, "run_resumed", {})

    def load_resumable_batches(self, run_id: str) -> list[TransferBatch]:
        batch_query = (
            select(transfer_batches.c.batch_id)
            .where(
                transfer_batches.c.run_id == run_id,
                transfer_batches.c.batch_status.in_(RESUMABLE_BATCH_STATUSES),
            )
            .order_by(transfer_batches.c.batch_id)
        )
        file_query = (
            select(transfer_files)
            .where(
                transfer_files.c.run_id == run_id,
                transfer_files.c.file_status.not_in(FINISHED_FILE_STATUSES),
            )
            .order_by(transfer_files.c.source_path)
        )
        with self.database_engine.connect() as database_connection:
            resumable_batch_ids = database_connection.execute(batch_query).scalars().all()
            unfinished_file_rows = database_connection.execute(file_query).mappings().all()

        batches_by_id = {batch_id: TransferBatch(batch_id=batch_id) for batch_id in resumable_batch_ids}
        for file_row in unfinished_file_rows:
            matching_batch = batches_by_id.get(file_row["batch_id"])
            if matching_batch:
                matching_batch.manifest_entries.append(self._build_manifest_entry(file_row))
        return [
            transfer_batch for transfer_batch in batches_by_id.values() if transfer_batch.manifest_entries
        ]

    def load_batch(self, run_id: str, batch_id: str) -> TransferBatch | None:
        for transfer_batch in self.load_resumable_batches(run_id):
            if transfer_batch.batch_id == batch_id:
                return transfer_batch
        return None

    def list_resumable_batch_ids(self, run_id: str) -> list[str]:
        return [transfer_batch.batch_id for transfer_batch in self.load_resumable_batches(run_id)]

    def count_unfinished_batches(self, run_id: str) -> int:
        with self.database_engine.connect() as database_connection:
            return database_connection.execute(
                select(func.count())
                .select_from(transfer_batches)
                .where(
                    transfer_batches.c.run_id == run_id,
                    transfer_batches.c.batch_status != BATCH_DONE,
                )
            ).scalar_one()

    def load_ingested_files(self, destination_location: str) -> list[dict]:
        ingested_query = (
            select(
                transfer_files.c.case_id,
                transfer_files.c.file_type,
                transfer_files.c.source_checksum,
                transfer_files.c.destination_path,
                transfer_files.c.processed_path,
            )
            .join(pipeline_runs, pipeline_runs.c.run_id == transfer_files.c.run_id)
            .where(
                pipeline_runs.c.destination_location == destination_location,
                transfer_files.c.file_status == FILE_DONE,
            )
            .distinct()
            .order_by(transfer_files.c.case_id, transfer_files.c.file_type)
        )
        with self.database_engine.connect() as database_connection:
            return [
                dict(file_row) for file_row in database_connection.execute(ingested_query).mappings().all()
            ]

    def record_audit_event(self, run_id: str, event_type: str, event_detail: dict) -> None:
        with self.database_engine.begin() as database_connection:
            self._insert_audit_event(database_connection, run_id, event_type, event_detail)

    def load_finished_source_keys(self, source_location: str) -> set[tuple[str, int]]:
        completed_query = (
            select(transfer_files.c.source_path, transfer_files.c.size_bytes)
            .join(pipeline_runs, pipeline_runs.c.run_id == transfer_files.c.run_id)
            .where(
                pipeline_runs.c.source_location == source_location,
                transfer_files.c.file_status.in_(FINISHED_FILE_STATUSES),
            )
        )
        with self.database_engine.connect() as database_connection:
            completed_rows = database_connection.execute(completed_query).all()
        return {(source_path, size_bytes) for source_path, size_bytes in completed_rows}

    def get_run(self, run_id: str) -> dict | None:
        with self.database_engine.connect() as database_connection:
            run_row = (
                database_connection.execute(select(pipeline_runs).where(pipeline_runs.c.run_id == run_id))
                .mappings()
                .first()
            )
        return dict(run_row) if run_row else None

    def find_latest_run_id(self) -> str | None:
        with self.database_engine.connect() as database_connection:
            return database_connection.execute(
                select(pipeline_runs.c.run_id).order_by(pipeline_runs.c.started_at.desc()).limit(1)
            ).scalar()

    def summarize_run(self, run_id: str) -> dict:
        with self.database_engine.connect() as database_connection:
            batch_counts = database_connection.execute(
                select(transfer_batches.c.batch_status, func.count())
                .where(transfer_batches.c.run_id == run_id)
                .group_by(transfer_batches.c.batch_status)
            ).all()
            file_counts = database_connection.execute(
                select(
                    transfer_files.c.file_status,
                    func.count(),
                    func.coalesce(func.sum(transfer_files.c.size_bytes), 0),
                )
                .where(transfer_files.c.run_id == run_id)
                .group_by(transfer_files.c.file_status)
            ).all()
            failed_files = database_connection.execute(
                select(
                    transfer_files.c.source_path, transfer_files.c.file_status, transfer_files.c.status_reason
                )
                .where(transfer_files.c.run_id == run_id, transfer_files.c.file_status != FILE_DONE)
                .order_by(transfer_files.c.source_path)
            ).all()

        return {
            "batches_by_status": {batch_status: batch_count for batch_status, batch_count in batch_counts},
            "files_by_status": {file_status: file_count for file_status, file_count, _ in file_counts},
            "bytes_by_status": {
                file_status: int(status_bytes) for file_status, _, status_bytes in file_counts
            },
            "problem_files": [
                {"source_path": source_path, "file_status": file_status, "reason": status_reason}
                for source_path, file_status, status_reason in failed_files
            ],
        }

    def trace_destination_file(self, destination_path: str) -> list[dict]:
        trace_query = (
            select(
                transfer_files.c.run_id,
                transfer_files.c.batch_id,
                transfer_files.c.source_path,
                transfer_files.c.source_checksum,
                transfer_files.c.destination_path,
                transfer_files.c.processed_path,
                transfer_files.c.updated_at,
                pipeline_runs.c.code_version,
                pipeline_runs.c.source_location,
            )
            .join(pipeline_runs, pipeline_runs.c.run_id == transfer_files.c.run_id)
            .where(
                transfer_files.c.file_status == FILE_DONE,
                or_(
                    transfer_files.c.destination_path == destination_path,
                    transfer_files.c.processed_path == destination_path,
                ),
            )
            .order_by(transfer_files.c.updated_at)
        )
        with self.database_engine.connect() as database_connection:
            return [
                dict(trace_row) for trace_row in database_connection.execute(trace_query).mappings().all()
            ]

    @staticmethod
    def _build_pending_file_row(
        run_id: str, batch_id: str, manifest_entry: ManifestEntry, saved_at: datetime
    ) -> dict:
        return {
            "run_id": run_id,
            "batch_id": batch_id,
            "source_path": manifest_entry.relative_path,
            "size_bytes": manifest_entry.size_bytes,
            "file_type": manifest_entry.file_type,
            "file_extension": manifest_entry.file_extension,
            "case_id": manifest_entry.case_id,
            "file_status": FILE_PENDING,
            "updated_at": saved_at,
        }

    @staticmethod
    def _build_manifest_entry(file_row) -> ManifestEntry:
        return ManifestEntry(
            relative_path=file_row["source_path"],
            size_bytes=file_row["size_bytes"],
            file_type=file_row["file_type"],
            file_extension=file_row["file_extension"],
            case_id=file_row["case_id"],
        )

    @staticmethod
    def _insert_audit_event(
        database_connection,
        run_id: str,
        event_type: str,
        event_detail: dict,
        batch_id: str | None = None,
    ) -> None:
        database_connection.execute(
            insert(audit_events).values(
                run_id=run_id,
                batch_id=batch_id,
                event_type=event_type,
                event_detail=json.dumps(event_detail, sort_keys=True, default=str),
                created_at=current_utc_time(),
            )
        )
