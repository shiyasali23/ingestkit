import hashlib
import io
import pickle

import boto3
import pytest
from moto import mock_aws

from ingestkit.file_checksum import calculate_stream_checksum
from ingestkit.pipeline_errors import FatalError, QuarantineError
from ingestkit.pipeline_runner import PipelineRunner
from ingestkit.s3_storage import S3Storage
from ingestkit.slide_reader import SlideReader
from ingestkit.state_schema import RUN_COMPLETED
from ingestkit.storage_factory import create_storage
from tests.conftest import write_sample_slide

BUCKET_NAME = "ingestkit-test"


@pytest.fixture
def s3_storage(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    with mock_aws():
        boto3.client("s3").create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        yield S3Storage(bucket_name=BUCKET_NAME, key_prefix="lab_upload")


def test_large_upload_is_split_into_parts_and_read_back(s3_storage):
    file_content = b"genomics-data" * 1_000_000
    bytes_written = s3_storage.write_from_stream(
        "C-0001.vcf", io.BytesIO(file_content), chunk_size=8 * 1024 * 1024
    )

    assert bytes_written == len(file_content)
    with s3_storage.open_reader("C-0001.vcf") as stored_stream:
        assert calculate_stream_checksum(stored_stream) == hashlib.sha256(file_content).hexdigest()


def test_files_are_listed_relative_to_prefix(s3_storage):
    s3_storage.write_from_stream("C-0001.svs", io.BytesIO(b"slide"), chunk_size=1024)
    s3_storage.write_from_stream("nested/C-0002.json", io.BytesIO(b"{}"), chunk_size=1024)

    listed_paths = sorted(stored_file.relative_path for stored_file in s3_storage.list_files())
    assert listed_paths == ["C-0001.svs", "nested/C-0002.json"]


def test_range_reader_supports_seek(s3_storage):
    s3_storage.write_from_stream("letters.txt", io.BytesIO(b"abcdefghij"), chunk_size=1024)
    with s3_storage.open_reader("letters.txt") as stored_stream:
        stored_stream.seek(4)
        assert stored_stream.read(3) == b"efg"
        stored_stream.seek(-2, io.SEEK_END)
        assert stored_stream.read() == b"ij"


def test_slide_metadata_is_read_directly_from_s3(s3_storage, tmp_path):
    slide_path = write_sample_slide(tmp_path / "C-0001.tiff", random_seed=3)
    with open(slide_path, "rb") as slide_file:
        s3_storage.write_from_stream("C-0001.tiff", slide_file, chunk_size=1024)

    with s3_storage.open_reader("C-0001.tiff") as stored_stream:
        slide_metadata = SlideReader().read_slide_metadata(stored_stream, case_id="C-0001")
    assert slide_metadata["width_pixels"] == 256


def test_move_and_delete_folder(s3_storage):
    s3_storage.write_from_stream("staging/run-1/a.vcf", io.BytesIO(b"a"), chunk_size=1024)
    s3_storage.write_from_stream("staging/run-1/b.vcf", io.BytesIO(b"b"), chunk_size=1024)

    s3_storage.move_file("staging/run-1/a.vcf", "raw/genomics/a.vcf")
    s3_storage.delete_folder("staging/run-1")

    assert s3_storage.file_exists("raw/genomics/a.vcf")
    assert not s3_storage.file_exists("staging/run-1/b.vcf")


def test_missing_object_raises_quarantine_error(s3_storage):
    with pytest.raises(QuarantineError):
        s3_storage.open_reader("missing.svs")


def test_path_escape_is_rejected(s3_storage):
    with pytest.raises(FatalError):
        s3_storage.file_exists("../other_bucket_prefix/file.svs")


def test_storage_can_be_pickled_for_worker_processes(s3_storage):
    s3_storage.file_exists("anything.svs")
    copied_storage = pickle.loads(pickle.dumps(s3_storage))
    assert copied_storage.bucket_name == BUCKET_NAME
    assert copied_storage._s3_client is None


def test_factory_builds_the_right_storage(tmp_path):
    s3_location = create_storage("s3://my-bucket/lab_upload")
    assert isinstance(s3_location, S3Storage)
    assert s3_location.describe_location("C-0001.svs") == "s3://my-bucket/lab_upload/C-0001.svs"
    assert create_storage(str(tmp_path)).describe_location("") == str(tmp_path)


def test_full_pipeline_runs_from_s3_to_s3(s3_storage, pipeline_config, state_store, source_folder):
    for source_file in sorted(source_folder.rglob("*")):
        if source_file.is_file():
            with open(source_file, "rb") as file_stream:
                relative_path = source_file.relative_to(source_folder).as_posix()
                s3_storage.write_from_stream(relative_path, file_stream, chunk_size=1024)

    s3_config = pipeline_config.model_copy(
        update={
            "source_path": f"s3://{BUCKET_NAME}/lab_upload",
            "destination_path": f"s3://{BUCKET_NAME}/destination",
        }
    )
    pipeline_runner = PipelineRunner(s3_config, state_store)
    run_id = pipeline_runner.start_run()

    assert state_store.get_run(run_id)["run_status"] == RUN_COMPLETED
    assert state_store.summarize_run(run_id)["files_by_status"] == {"done": 6}
    destination_storage = create_storage(f"s3://{BUCKET_NAME}/destination")
    stored_paths = {stored_file.relative_path for stored_file in destination_storage.list_files()}
    assert "processed/cases/cases.parquet" in stored_paths
    assert not any(stored_path.startswith("staging/") for stored_path in stored_paths)
