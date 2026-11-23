import heapq
import math
from dataclasses import dataclass, field

from ingestkit.file_inventory import ManifestEntry


@dataclass
class TransferBatch:
    batch_id: str
    manifest_entries: list[ManifestEntry] = field(default_factory=list)

    @property
    def total_bytes(self) -> int:
        return sum(entry.size_bytes for entry in self.manifest_entries)

    @property
    def file_count(self) -> int:
        return len(self.manifest_entries)


class BatchPlanner:
    def __init__(self, batch_size_bytes: int):
        if batch_size_bytes <= 0:
            raise ValueError("batch_size_bytes must be greater than zero")
        self.batch_size_bytes = batch_size_bytes

    def count_batches(self, manifest_entries: list[ManifestEntry], parallel_slots: int) -> int:
        total_bytes = sum(manifest_entry.size_bytes for manifest_entry in manifest_entries)
        batches_by_size = math.ceil(total_bytes / self.batch_size_bytes)
        return max(1, min(len(manifest_entries), max(parallel_slots, batches_by_size)))

    def plan_batches(
        self, manifest_entries: list[ManifestEntry], parallel_slots: int = 1
    ) -> list[TransferBatch]:
        if not manifest_entries:
            return []

        batch_count = self.count_batches(manifest_entries, parallel_slots)
        entry_groups: list[list[ManifestEntry]] = [[] for _ in range(batch_count)]
        lightest_groups = [(0, group_index) for group_index in range(batch_count)]

        # largest file first, always into the lightest batch, so every worker gets a similar amount of bytes
        largest_first = sorted(
            manifest_entries,
            key=lambda manifest_entry: (-manifest_entry.size_bytes, manifest_entry.relative_path),
        )
        for manifest_entry in largest_first:
            group_bytes, group_index = heapq.heappop(lightest_groups)
            entry_groups[group_index].append(manifest_entry)
            heapq.heappush(lightest_groups, (group_bytes + manifest_entry.size_bytes, group_index))

        return [
            TransferBatch(
                batch_id=f"batch-{batch_number:05d}",
                manifest_entries=sorted(entry_group, key=lambda manifest_entry: manifest_entry.relative_path),
            )
            for batch_number, entry_group in enumerate(entry_groups, start=1)
        ]
