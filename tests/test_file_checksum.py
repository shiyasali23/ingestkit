import hashlib
import io

from ingestkit.file_checksum import ChecksumReader, calculate_stream_checksum


def test_checksum_matches_hashlib():
    file_content = b"genomics" * 10_000
    expected_checksum = hashlib.sha256(file_content).hexdigest()
    assert calculate_stream_checksum(io.BytesIO(file_content), chunk_size=1024) == expected_checksum


def test_checksum_is_same_for_any_chunk_size():
    file_content = bytes(range(256)) * 500
    small_chunk_checksum = calculate_stream_checksum(io.BytesIO(file_content), chunk_size=3)
    large_chunk_checksum = calculate_stream_checksum(io.BytesIO(file_content), chunk_size=1_000_000)
    assert small_chunk_checksum == large_chunk_checksum


def test_checksum_reader_counts_bytes_while_copying():
    file_content = b"slide-data" * 123
    checksum_reader = ChecksumReader(io.BytesIO(file_content))
    copied_content = b""
    while file_chunk := checksum_reader.read(50):
        copied_content += file_chunk
    assert copied_content == file_content
    assert checksum_reader.bytes_read == len(file_content)
    assert checksum_reader.checksum_value == hashlib.sha256(file_content).hexdigest()


def test_empty_stream_has_known_checksum():
    assert calculate_stream_checksum(io.BytesIO(b"")) == hashlib.sha256(b"").hexdigest()
