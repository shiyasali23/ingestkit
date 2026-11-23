import hashlib
from typing import BinaryIO

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class ChecksumReader:
    def __init__(self, source_stream: BinaryIO):
        self.source_stream = source_stream
        self.sha256_hash = hashlib.sha256()
        self.bytes_read = 0

    def read(self, chunk_size: int = -1) -> bytes:
        file_chunk = self.source_stream.read(chunk_size)
        self.sha256_hash.update(file_chunk)
        self.bytes_read += len(file_chunk)
        return file_chunk

    @property
    def checksum_value(self) -> str:
        return self.sha256_hash.hexdigest()


def calculate_stream_checksum(source_stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    checksum_reader = ChecksumReader(source_stream)
    file_chunk = checksum_reader.read(chunk_size)
    while file_chunk:
        file_chunk = checksum_reader.read(chunk_size)
    return checksum_reader.checksum_value
