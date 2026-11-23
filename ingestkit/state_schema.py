from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

RUN_RUNNING = "running"
RUN_COMPLETED = "completed"
RUN_COMPLETED_WITH_FAILURES = "completed_with_failures"
RUN_INTERRUPTED = "interrupted"
RUN_FAILED = "failed"

BATCH_PENDING = "pending"
BATCH_RUNNING = "running"
BATCH_DONE = "done"
BATCH_FAILED = "failed"
RESUMABLE_BATCH_STATUSES = (BATCH_PENDING, BATCH_RUNNING, BATCH_FAILED)

FILE_PENDING = "pending"
FILE_DONE = "done"
FILE_QUARANTINED = "quarantined"
FILE_FAILED = "failed"
FINISHED_FILE_STATUSES = (FILE_DONE, FILE_QUARANTINED)

state_metadata = MetaData()

pipeline_runs = Table(
    "pipeline_runs",
    state_metadata,
    Column("run_id", String(64), primary_key=True),
    Column("run_status", String(32), nullable=False),
    Column("runner_type", String(32), nullable=False),
    Column("source_location", Text, nullable=False),
    Column("destination_location", Text, nullable=False),
    Column("code_version", String(64), nullable=False),
    Column("config_snapshot", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
)

transfer_batches = Table(
    "transfer_batches",
    state_metadata,
    Column("run_id", String(64), primary_key=True),
    Column("batch_id", String(32), primary_key=True),
    Column("batch_status", String(32), nullable=False),
    Column("attempt_count", Integer, nullable=False, default=0),
    Column("total_bytes", BigInteger, nullable=False),
    Column("file_count", Integer, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(["run_id"], ["pipeline_runs.run_id"]),
)

transfer_files = Table(
    "transfer_files",
    state_metadata,
    Column("file_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("batch_id", String(32), nullable=False),
    Column("source_path", Text, nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("file_type", String(32), nullable=False),
    Column("file_extension", String(32), nullable=False),
    Column("case_id", String(128), nullable=False),
    Column("file_status", String(32), nullable=False),
    Column("source_checksum", String(64)),
    Column("destination_path", Text),
    Column("processed_path", Text),
    Column("status_reason", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("run_id", "source_path", name="unique_file_per_run"),
    ForeignKeyConstraint(["run_id", "batch_id"], ["transfer_batches.run_id", "transfer_batches.batch_id"]),
)

audit_events = Table(
    "audit_events",
    state_metadata,
    Column("event_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("batch_id", String(32)),
    Column("event_type", String(64), nullable=False),
    Column("event_detail", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
