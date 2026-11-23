import pytest

from ingestkit.batch_planner import BatchPlanner
from ingestkit.file_inventory import ManifestEntry


def build_entry(file_name: str, size_bytes: int) -> ManifestEntry:
    return ManifestEntry(
        relative_path=file_name,
        size_bytes=size_bytes,
        file_type="slide",
        file_extension=".svs",
        case_id=file_name.split(".")[0],
    )


def test_small_data_is_still_split_across_all_workers():
    manifest_entries = [build_entry(f"C-{number:04d}.svs", 10) for number in range(12)]
    planned_batches = BatchPlanner(batch_size_bytes=1000).plan_batches(manifest_entries, parallel_slots=4)

    assert len(planned_batches) == 4
    assert [transfer_batch.file_count for transfer_batch in planned_batches] == [3, 3, 3, 3]


def test_large_data_uses_more_batches_than_workers():
    manifest_entries = [build_entry(f"C-{number:04d}.svs", 30) for number in range(10)]
    planned_batches = BatchPlanner(batch_size_bytes=100).plan_batches(manifest_entries, parallel_slots=2)

    assert len(planned_batches) == 3
    assert sum(transfer_batch.file_count for transfer_batch in planned_batches) == 10


def test_batches_are_balanced_by_bytes_not_file_count():
    manifest_entries = [build_entry("A.svs", 90), build_entry("B.svs", 60), build_entry("C.svs", 30)]
    manifest_entries += [build_entry(f"V-{number:02d}.svs", 1) for number in range(30)]
    planned_batches = BatchPlanner(batch_size_bytes=1000).plan_batches(manifest_entries, parallel_slots=2)

    batch_sizes = sorted(transfer_batch.total_bytes for transfer_batch in planned_batches)
    assert batch_sizes == [105, 105]


def test_never_more_batches_than_files():
    manifest_entries = [build_entry("A.svs", 10), build_entry("B.svs", 10)]
    planned_batches = BatchPlanner(batch_size_bytes=100).plan_batches(manifest_entries, parallel_slots=8)

    assert len(planned_batches) == 2


def test_empty_manifest_gives_no_batches():
    assert BatchPlanner(batch_size_bytes=100).plan_batches([], parallel_slots=4) == []


def test_planning_is_deterministic():
    manifest_entries = [build_entry(f"C-{number:04d}.svs", number * 7 + 1) for number in range(20)]
    first_plan = BatchPlanner(batch_size_bytes=50).plan_batches(manifest_entries, parallel_slots=3)
    second_plan = BatchPlanner(batch_size_bytes=50).plan_batches(
        list(reversed(manifest_entries)), parallel_slots=3
    )

    first_layout = [
        (batch.batch_id, [entry.relative_path for entry in batch.manifest_entries]) for batch in first_plan
    ]
    second_layout = [
        (batch.batch_id, [entry.relative_path for entry in batch.manifest_entries]) for batch in second_plan
    ]
    assert first_layout == second_layout


def test_batch_ids_are_unique_and_ordered():
    manifest_entries = [build_entry(f"C-{number:04d}.svs", 60) for number in range(5)]
    batch_ids = [
        batch.batch_id for batch in BatchPlanner(batch_size_bytes=100).plan_batches(manifest_entries)
    ]
    assert batch_ids == ["batch-00001", "batch-00002", "batch-00003"]


def test_zero_batch_size_is_rejected():
    with pytest.raises(ValueError):
        BatchPlanner(batch_size_bytes=0)
