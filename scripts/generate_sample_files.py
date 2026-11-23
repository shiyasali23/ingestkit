import argparse
import json
import random
from pathlib import Path

import numpy
import tifffile

PANEL_GENES = {
    "SF3B1": "chr2",
    "TP53": "chr17",
    "ASXL1": "chr20",
    "TET2": "chr4",
    "DNMT3A": "chr2",
    "RUNX1": "chr21",
}
DIAGNOSES = ["MDS", "AML", "CLL", "control"]
FAKE_FIRST_NAMES = ["Anna", "Lukas", "Mira", "Jonas", "Sara", "Tom"]


def write_sample_slide(slide_path: Path, edge_pixels: int, random_generator: numpy.random.Generator) -> None:
    # A pyramid with three levels, like a real scanner file, so the slide reader sees zoom levels.
    base_pixels = random_generator.integers(180, 255, size=(edge_pixels, edge_pixels, 3), dtype=numpy.uint8)
    with tifffile.TiffWriter(slide_path) as tiff_writer:
        tiff_writer.write(base_pixels, tile=(256, 256), photometric="rgb", subifds=2)
        for downsample_factor in (4, 16):
            tiff_writer.write(
                base_pixels[::downsample_factor, ::downsample_factor], tile=(256, 256), subfiletype=1
            )


def build_variant_lines(case_id: str, random_generator: random.Random) -> list[str]:
    variant_lines = []
    for gene_name in random_generator.sample(list(PANEL_GENES), k=random_generator.randint(0, 3)):
        variant_position = random_generator.randint(1_000_000, 200_000_000)
        allele_fraction = round(random_generator.uniform(0.02, 0.5), 2)
        read_depth = random_generator.randint(50, 900)
        filter_value = "PASS" if read_depth >= 100 else "LowDepth"
        info_text = f"GENE={gene_name};VAF={allele_fraction};DP={read_depth};CASE={case_id}"
        variant_lines.append(
            f"{PANEL_GENES[gene_name]}\t{variant_position}\t.\tC\tT\t60\t{filter_value}\t{info_text}"
        )
    return variant_lines


def write_sample_vcf(vcf_path: Path, case_id: str, random_generator: random.Random) -> None:
    header_lines = [
        "##fileformat=VCFv4.2",
        "##source=ingestkit-synthetic",
        '##INFO=<ID=VAF,Number=1,Type=Float,Description="Variant allele fraction">',
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
    ]
    vcf_path.write_text("\n".join(header_lines + build_variant_lines(case_id, random_generator)) + "\n")


def build_fake_birth_date(random_generator: random.Random) -> str:
    birth_year = random_generator.randint(1940, 1979)
    birth_month = random_generator.randint(1, 12)
    birth_day = random_generator.randint(1, 28)
    return f"{birth_year}-{birth_month:02d}-{birth_day:02d}"


def write_sample_clinical(clinical_path: Path, case_id: str, random_generator: random.Random) -> None:
    clinical_record = {
        "case_id": case_id,
        "patient_id": f"HOSP-{random_generator.randint(100000, 999999)}",
        "name": f"{random_generator.choice(FAKE_FIRST_NAMES)} Example",
        "birth_date": build_fake_birth_date(random_generator),
        "age": random_generator.randint(45, 85),
        "sex": random_generator.choice(["female", "male"]),
        "diagnosis": random_generator.choice(DIAGNOSES),
        "hemoglobin_g_dl": round(random_generator.uniform(7.5, 15.5), 1),
        "synthetic_data": True,
    }
    clinical_path.write_text(json.dumps(clinical_record, indent=2))


def generate_sample_files(
    output_path: str, case_count: int, slide_edge_pixels: int, random_seed: int
) -> None:
    output_folder = Path(output_path)
    output_folder.mkdir(parents=True, exist_ok=True)
    random_generator = random.Random(random_seed)
    pixel_generator = numpy.random.default_rng(random_seed)

    for case_number in range(1, case_count + 1):
        case_id = f"C-{case_number:04d}"
        write_sample_slide(output_folder / f"{case_id}.tiff", slide_edge_pixels, pixel_generator)
        if case_number % 7 != 0:
            write_sample_vcf(output_folder / f"{case_id}.vcf", case_id, random_generator)
        write_sample_clinical(output_folder / f"{case_id}.json", case_id, random_generator)

    (output_folder / "C-9001.vcf").write_text("")
    (output_folder / "C-9002.json").write_text('{"case_id": "C-9002", "age": 70}')
    (output_folder / "scanner_notes.docx").write_bytes(b"not a supported file type")
    print(f"Created {case_count} synthetic cases and 3 problem files in {output_folder}")
    print("Every 7th case has no VCF, to show incomplete cases in the case table.")


def main() -> None:
    argument_parser = argparse.ArgumentParser(description="Create synthetic slides, VCF and clinical files.")
    argument_parser.add_argument("--output-path", default="./sample_data/lab_upload")
    argument_parser.add_argument("--case-count", type=int, default=20)
    argument_parser.add_argument("--slide-edge-pixels", type=int, default=4096)
    argument_parser.add_argument("--random-seed", type=int, default=42)
    parsed_arguments = argument_parser.parse_args()
    generate_sample_files(
        output_path=parsed_arguments.output_path,
        case_count=parsed_arguments.case_count,
        slide_edge_pixels=parsed_arguments.slide_edge_pixels,
        random_seed=parsed_arguments.random_seed,
    )


if __name__ == "__main__":
    main()
