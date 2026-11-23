from typing import BinaryIO

import tiffslide

from ingestkit.pipeline_errors import QuarantineError

CHECK_TILE_SIZE = 64


class SlideReader:
    def read_slide_metadata(self, slide_stream: BinaryIO, case_id: str) -> dict:
        try:
            with tiffslide.TiffSlide(slide_stream) as opened_slide:
                slide_width, slide_height = opened_slide.dimensions
                self._read_check_tile(opened_slide)
                slide_properties = opened_slide.properties
                return {
                    "case_id": case_id,
                    "width_pixels": int(slide_width),
                    "height_pixels": int(slide_height),
                    "level_count": int(opened_slide.level_count),
                    "objective_power": self._read_number(slide_properties.get("tiffslide.objective-power")),
                    "microns_per_pixel": self._read_number(slide_properties.get("tiffslide.mpp-x")),
                    "scanner_vendor": str(slide_properties.get("tiffslide.vendor") or "unknown"),
                }
        except Exception as e:
            raise QuarantineError(f"Slide could not be opened: {e}") from e

    @staticmethod
    def _read_check_tile(opened_slide: tiffslide.TiffSlide) -> None:
        # Reading one small tile from the smallest level proves the pixel data is readable
        # without pulling a gigapixel image into memory.
        smallest_level = opened_slide.level_count - 1
        level_width, level_height = opened_slide.level_dimensions[smallest_level]
        tile_size = (min(CHECK_TILE_SIZE, level_width), min(CHECK_TILE_SIZE, level_height))
        opened_slide.read_region((0, 0), smallest_level, tile_size)

    @staticmethod
    def _read_number(property_value) -> float | None:
        if property_value in (None, ""):
            return None
        try:
            return float(property_value)
        except (TypeError, ValueError):
            return None
