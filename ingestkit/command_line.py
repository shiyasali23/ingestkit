import argparse
import json
import sys

from ingestkit.pipeline_config import load_pipeline_config
from ingestkit.pipeline_errors import ConfigError, PipelineError
from ingestkit.pipeline_logging import configure_pipeline_logging
from ingestkit.pipeline_runner import PipelineRunner
from ingestkit.state_schema import RUN_COMPLETED

EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERRUPTED = 130
BYTES_PER_GB = 1024**3


def build_argument_parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(
        prog="ingestkit",
        description="Pipeline for large scale multimodal data to move from one place to another.",
    )
    command_parsers = argument_parser.add_subparsers(dest="command_name", required=True)

    plan_parser = command_parsers.add_parser(
        "plan", help="Show what would be transferred, without moving files"
    )
    run_parser = command_parsers.add_parser("run", help="Start a new run")
    resume_parser = command_parsers.add_parser("resume", help="Continue a run from where it stopped")
    status_parser = command_parsers.add_parser("status", help="Show the state of a run")
    worker_parser = command_parsers.add_parser("worker", help="Run one batch (used by Kubernetes Jobs)")
    trace_parser = command_parsers.add_parser("trace", help="Show where a stored file came from")

    for command_parser in (
        plan_parser,
        run_parser,
        resume_parser,
        status_parser,
        worker_parser,
        trace_parser,
    ):
        command_parser.add_argument("--config", dest="config_path", default="pipeline_config.yaml")
        command_parser.add_argument("--log-level", dest="log_level", default="INFO")

    resume_parser.add_argument("--run-id", dest="run_id", required=True)
    status_parser.add_argument("--run-id", dest="run_id", default=None, help="Defaults to the latest run")
    worker_parser.add_argument("--run-id", dest="run_id", required=True)
    worker_parser.add_argument("--batch-id", dest="batch_id", required=True)
    trace_parser.add_argument("--path", dest="stored_path", required=True, help="e.g. raw/slide/<sha256>.svs")
    return argument_parser


def print_run_plan(pipeline_runner: PipelineRunner) -> None:
    run_plan = pipeline_runner.plan_run()
    print(f"Files to transfer : {run_plan.planned_file_count}")
    print(f"Data to transfer  : {run_plan.planned_bytes / BYTES_PER_GB:.2f} GB")
    print(f"Parallel workers  : {run_plan.parallel_slots}")
    print(f"Batches           : {len(run_plan.planned_batches)}")
    print(f"Already handled   : {run_plan.skipped_file_count} files (skipped)")
    for transfer_batch in run_plan.planned_batches:
        print(
            f"  {transfer_batch.batch_id}  {transfer_batch.file_count:>6} files  "
            f"{transfer_batch.total_bytes / BYTES_PER_GB:>8.2f} GB"
        )


def print_run_status(pipeline_runner: PipelineRunner, run_id: str | None) -> int:
    selected_run_id = run_id or pipeline_runner.state_store.find_latest_run_id()
    run_record = pipeline_runner.state_store.get_run(selected_run_id) if selected_run_id else None
    if not run_record:
        print("No run found.")
        return EXIT_FAILURE

    run_summary = pipeline_runner.state_store.summarize_run(selected_run_id)
    print(f"Run          : {run_record['run_id']}")
    print(f"Status       : {run_record['run_status']}")
    print(f"Code version : {run_record['code_version']}")
    print(f"Started      : {run_record['started_at']}")
    print(f"Finished     : {run_record['finished_at'] or '-'}")
    print(f"Batches      : {run_summary['batches_by_status']}")
    print(f"Files        : {run_summary['files_by_status']}")
    transferred_gb = run_summary["bytes_by_status"].get("done", 0) / BYTES_PER_GB
    print(f"Transferred  : {transferred_gb:.2f} GB")
    for problem_file in run_summary["problem_files"]:
        print(f"  [{problem_file['file_status']}] {problem_file['source_path']}: {problem_file['reason']}")
    return EXIT_SUCCESS if run_record["run_status"] == RUN_COMPLETED else EXIT_FAILURE


def print_file_trace(pipeline_runner: PipelineRunner, stored_path: str) -> int:
    trace_rows = pipeline_runner.state_store.trace_destination_file(stored_path)
    if not trace_rows:
        print(f"No lineage found for {stored_path}")
        return EXIT_FAILURE
    for trace_row in trace_rows:
        print(json.dumps(trace_row, indent=2, default=str))
    return EXIT_SUCCESS


def run_command(parsed_arguments: argparse.Namespace) -> int:
    configure_pipeline_logging(parsed_arguments.log_level)
    pipeline_config = load_pipeline_config(parsed_arguments.config_path)
    pipeline_runner = PipelineRunner(pipeline_config, log_level=parsed_arguments.log_level)
    command_name = parsed_arguments.command_name

    if command_name == "plan":
        print_run_plan(pipeline_runner)
        return EXIT_SUCCESS
    if command_name == "status":
        return print_run_status(pipeline_runner, parsed_arguments.run_id)
    if command_name == "trace":
        return print_file_trace(pipeline_runner, parsed_arguments.stored_path)
    if command_name == "worker":
        batch_succeeded = pipeline_runner.run_single_batch(parsed_arguments.run_id, parsed_arguments.batch_id)
        return EXIT_SUCCESS if batch_succeeded else EXIT_FAILURE

    if command_name == "run":
        finished_run_id = pipeline_runner.start_run()
    else:
        finished_run_id = pipeline_runner.resume_run(parsed_arguments.run_id)
    return print_run_status(pipeline_runner, finished_run_id)


def main() -> None:
    parsed_arguments = build_argument_parser().parse_args()
    try:
        exit_code = run_command(parsed_arguments)
    except KeyboardInterrupt:
        print("Interrupted. Continue later with: ingestkit resume --run-id <run_id>", file=sys.stderr)
        exit_code = EXIT_INTERRUPTED
    except ConfigError as e:
        print(f"Config error: {e}", file=sys.stderr)
        exit_code = EXIT_CONFIG_ERROR
    except PipelineError as e:
        print(f"Error: {e}", file=sys.stderr)
        exit_code = EXIT_FAILURE
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
