from ingestkit.file_inventory import UNKNOWN_CASE_ID, UNKNOWN_FILE_TYPE, FileInventory
from ingestkit.local_storage import LocalStorage


def test_manifest_detects_types_and_case_ids(source_folder):
    manifest_entries = FileInventory("^([^._]+)").build_manifest(LocalStorage(str(source_folder)))
    entries_by_path = {entry.relative_path: entry for entry in manifest_entries}

    assert len(manifest_entries) == 6
    assert entries_by_path["C-0001.svs"].file_type == "slide"
    assert entries_by_path["C-0001.vcf"].file_type == "genomics"
    assert entries_by_path["C-0001.json"].file_type == "clinical"
    assert entries_by_path["nested/C-0003.json"].case_id == "C-0003"
    assert entries_by_path["C-0002.svs"].size_bytes == (source_folder / "C-0002.svs").stat().st_size


def test_double_extension_is_detected():
    assert FileInventory.detect_file_extension("C-0001.vcf.gz") == ".vcf.gz"
    assert FileInventory.detect_file_extension("C-0001.SVS") == ".svs"


def test_unknown_type_is_reported(tmp_path):
    (tmp_path / "notes.docx").write_bytes(b"text")
    manifest_entries = FileInventory("^([^._]+)").build_manifest(LocalStorage(str(tmp_path)))
    assert manifest_entries[0].file_type == UNKNOWN_FILE_TYPE


def test_case_id_falls_back_to_unknown():
    file_inventory = FileInventory("^(C-\\d{4})")
    assert file_inventory.extract_case_id("random_name.svs") == UNKNOWN_CASE_ID
    assert file_inventory.extract_case_id("folder/C-0042_scan.svs") == "C-0042"
