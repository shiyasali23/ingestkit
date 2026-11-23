import io

import pytest

from ingestkit.local_storage import LocalStorage
from ingestkit.pipeline_errors import FatalError, QuarantineError


def test_list_files_skips_hidden_and_partial_files(tmp_path):
    (tmp_path / "C-0001.svs").write_bytes(b"slide")
    (tmp_path / ".DS_Store").write_bytes(b"system")
    (tmp_path / "C-0002.svs.partial").write_bytes(b"half")
    (tmp_path / ".hidden_folder").mkdir()
    (tmp_path / ".hidden_folder" / "secret.svs").write_bytes(b"hidden")

    listed_paths = [stored_file.relative_path for stored_file in LocalStorage(str(tmp_path)).list_files()]
    assert listed_paths == ["C-0001.svs"]


def test_write_from_stream_creates_folders_and_leaves_no_partial(tmp_path):
    local_storage = LocalStorage(str(tmp_path))
    bytes_written = local_storage.write_from_stream(
        "raw/slide/file.svs", io.BytesIO(b"x" * 100), chunk_size=7
    )
    assert bytes_written == 100
    assert (tmp_path / "raw/slide/file.svs").read_bytes() == b"x" * 100
    assert not list(tmp_path.rglob("*.partial"))


def test_path_outside_storage_is_rejected(tmp_path):
    with pytest.raises(FatalError):
        LocalStorage(str(tmp_path)).open_reader("../outside.txt")


def test_missing_file_raises_quarantine_error(tmp_path):
    with pytest.raises(QuarantineError):
        LocalStorage(str(tmp_path)).open_reader("missing.svs")


def test_missing_storage_folder_raises_fatal_error(tmp_path):
    with pytest.raises(FatalError):
        list(LocalStorage(str(tmp_path / "missing_folder")).list_files())


def test_delete_folder_refuses_storage_root(tmp_path):
    with pytest.raises(FatalError):
        LocalStorage(str(tmp_path)).delete_folder(".")


def test_delete_folder_ignores_missing_folder(tmp_path):
    LocalStorage(str(tmp_path)).delete_folder("staging/missing_run")
