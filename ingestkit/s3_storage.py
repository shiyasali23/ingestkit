import io
from collections.abc import Iterator
from typing import BinaryIO

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

from ingestkit.pipeline_errors import FatalError, QuarantineError, RetryableError
from ingestkit.storage_backend import StorageBackend, StoredFile

MISSING_OBJECT_CODES = {"404", "NoSuchKey", "NotFound"}
DENIED_CODES = {"403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}
MIN_MULTIPART_BYTES = 8 * 1024 * 1024
UPLOAD_THREADS = 4
DELETE_BATCH_SIZE = 1000
READ_BUFFER_BYTES = 1024 * 1024


def translate_s3_error(s3_error: Exception, error_context: str) -> Exception:
    error_message = f"{error_context}: {s3_error}"
    if isinstance(s3_error, ClientError):
        error_code = str(s3_error.response.get("Error", {}).get("Code", ""))
        if error_code in MISSING_OBJECT_CODES:
            return QuarantineError(error_message)
        if error_code in DENIED_CODES:
            return FatalError(error_message)
    return RetryableError(error_message)


class S3RangeReader(io.RawIOBase):
    # Reads an S3 object with HTTP range requests, so a slide reader can seek around a
    # multi-GB file and only download the parts it needs.
    def __init__(self, s3_client, bucket_name: str, object_key: str, object_size: int):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.object_size = object_size
        self.read_position = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.read_position

    def seek(self, seek_offset: int, seek_origin: int = io.SEEK_SET) -> int:
        if seek_origin == io.SEEK_SET:
            self.read_position = seek_offset
        elif seek_origin == io.SEEK_CUR:
            self.read_position += seek_offset
        else:
            self.read_position = self.object_size + seek_offset
        self.read_position = max(0, self.read_position)
        return self.read_position

    def readinto(self, target_buffer) -> int:
        if self.read_position >= self.object_size:
            return 0
        last_byte = min(self.read_position + len(target_buffer), self.object_size) - 1
        try:
            range_response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.object_key,
                Range=f"bytes={self.read_position}-{last_byte}",
            )
            range_bytes = range_response["Body"].read()
        except (ClientError, BotoCoreError) as e:
            raise translate_s3_error(e, f"Error reading s3://{self.bucket_name}/{self.object_key}") from e

        target_buffer[: len(range_bytes)] = range_bytes
        self.read_position += len(range_bytes)
        return len(range_bytes)


class ByteCountingReader:
    def __init__(self, source_stream: BinaryIO):
        self.source_stream = source_stream
        self.bytes_read = 0

    def read(self, chunk_size: int = -1) -> bytes:
        file_chunk = self.source_stream.read(chunk_size)
        self.bytes_read += len(file_chunk)
        return file_chunk


class S3Storage(StorageBackend):
    def __init__(self, bucket_name: str, key_prefix: str = "", endpoint_url: str | None = None):
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix.strip("/") + "/" if key_prefix.strip("/") else ""
        self.endpoint_url = endpoint_url
        self._s3_client = None

    def __getstate__(self) -> dict:
        # boto3 clients cannot be pickled; each worker process builds its own.
        storage_state = self.__dict__.copy()
        storage_state["_s3_client"] = None
        return storage_state

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client("s3", endpoint_url=self.endpoint_url)
        return self._s3_client

    def list_files(self) -> Iterator[StoredFile]:
        page_iterator = self.s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket_name,
            Prefix=self.key_prefix,
        )
        try:
            for result_page in page_iterator:
                for stored_object in result_page.get("Contents", []):
                    relative_path = stored_object["Key"][len(self.key_prefix) :]
                    if not relative_path or relative_path.endswith("/") or relative_path.startswith("."):
                        continue
                    yield StoredFile(relative_path=relative_path, size_bytes=stored_object["Size"])
        except (ClientError, BotoCoreError) as e:
            raise translate_s3_error(e, f"Error listing s3://{self.bucket_name}/{self.key_prefix}") from e

    def open_reader(self, relative_path: str) -> BinaryIO:
        object_key = self._build_key(relative_path)
        object_size = self._read_object_size(object_key)
        range_reader = S3RangeReader(self.s3_client, self.bucket_name, object_key, object_size)
        return io.BufferedReader(range_reader, buffer_size=READ_BUFFER_BYTES)

    def write_from_stream(self, relative_path: str, source_stream: BinaryIO, chunk_size: int) -> int:
        counting_reader = ByteCountingReader(source_stream)
        transfer_settings = TransferConfig(
            multipart_threshold=max(chunk_size, MIN_MULTIPART_BYTES),
            multipart_chunksize=max(chunk_size, MIN_MULTIPART_BYTES),
            max_concurrency=UPLOAD_THREADS,
        )
        try:
            self.s3_client.upload_fileobj(
                counting_reader,
                self.bucket_name,
                self._build_key(relative_path),
                Config=transfer_settings,
            )
        except (ClientError, BotoCoreError) as e:
            raise translate_s3_error(e, f"Error writing {self.describe_location(relative_path)}") from e
        return counting_reader.bytes_read

    def move_file(self, source_relative_path: str, target_relative_path: str) -> None:
        copy_source = {"Bucket": self.bucket_name, "Key": self._build_key(source_relative_path)}
        try:
            self.s3_client.copy(copy_source, self.bucket_name, self._build_key(target_relative_path))
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=self._build_key(source_relative_path))
        except (ClientError, BotoCoreError) as e:
            raise translate_s3_error(
                e, f"Error moving {source_relative_path} to {target_relative_path}"
            ) from e

    def delete_file(self, relative_path: str) -> None:
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=self._build_key(relative_path))
        except (ClientError, BotoCoreError) as e:
            raise translate_s3_error(e, f"Error deleting {self.describe_location(relative_path)}") from e

    def delete_folder(self, relative_path: str) -> None:
        folder_prefix = self._build_key(relative_path).rstrip("/") + "/"
        if folder_prefix == "/" or folder_prefix == self.key_prefix:
            raise FatalError("Refusing to delete the storage root folder")

        page_iterator = self.s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket_name,
            Prefix=folder_prefix,
        )
        try:
            for result_page in page_iterator:
                object_keys = [
                    {"Key": stored_object["Key"]} for stored_object in result_page.get("Contents", [])
                ]
                for batch_start in range(0, len(object_keys), DELETE_BATCH_SIZE):
                    self.s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        Delete={"Objects": object_keys[batch_start : batch_start + DELETE_BATCH_SIZE]},
                    )
        except (ClientError, BotoCoreError) as e:
            raise translate_s3_error(e, f"Error deleting folder {folder_prefix}") from e

    def file_exists(self, relative_path: str) -> bool:
        try:
            self._read_object_size(self._build_key(relative_path))
            return True
        except QuarantineError:
            return False

    def describe_location(self, relative_path: str) -> str:
        return f"s3://{self.bucket_name}/{self._build_key(relative_path)}".rstrip("/")

    def _build_key(self, relative_path: str) -> str:
        if ".." in relative_path.split("/"):
            raise FatalError(f"Path escapes storage prefix: {relative_path}")
        return self.key_prefix + relative_path.lstrip("/")

    def _read_object_size(self, object_key: str) -> int:
        try:
            return self.s3_client.head_object(Bucket=self.bucket_name, Key=object_key)["ContentLength"]
        except (ClientError, BotoCoreError) as e:
            raise translate_s3_error(e, f"Error reading s3://{self.bucket_name}/{object_key}") from e
