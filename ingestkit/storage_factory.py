from ingestkit.local_storage import LocalStorage
from ingestkit.pipeline_errors import ConfigError
from ingestkit.s3_storage import S3Storage
from ingestkit.storage_backend import StorageBackend

S3_SCHEME = "s3://"


def create_storage(storage_location: str, s3_endpoint_url: str | None = None) -> StorageBackend:
    if not storage_location.startswith(S3_SCHEME):
        return LocalStorage(storage_location)

    bucket_and_prefix = storage_location[len(S3_SCHEME) :]
    bucket_name, _, key_prefix = bucket_and_prefix.partition("/")
    if not bucket_name:
        raise ConfigError(f"S3 location has no bucket name: {storage_location}")
    return S3Storage(bucket_name=bucket_name, key_prefix=key_prefix, endpoint_url=s3_endpoint_url)
