import io

import pytest

from ingestkit.pipeline_errors import QuarantineError
from ingestkit.slide_reader import SlideReader
from tests.conftest import write_sample_slide


def test_slide_metadata_is_read(tmp_path):
    slide_path = write_sample_slide(tmp_path / "C-0042.tiff", random_seed=7, edge_pixels=320)
    with open(slide_path, "rb") as slide_stream:
        slide_metadata = SlideReader().read_slide_metadata(slide_stream, case_id="C-0042")

    assert slide_metadata["case_id"] == "C-0042"
    assert slide_metadata["width_pixels"] == 320
    assert slide_metadata["height_pixels"] == 320
    assert slide_metadata["level_count"] >= 1


def test_random_bytes_are_quarantined():
    with pytest.raises(QuarantineError, match="Slide could not be opened"):
        SlideReader().read_slide_metadata(io.BytesIO(b"not a slide" * 100), case_id="C-0042")
