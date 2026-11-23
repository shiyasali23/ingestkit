import argparse
import json
import shutil
import urllib.parse
import urllib.request
from pathlib import Path

GDC_API_URL = "https://api.gdc.cancer.gov"
DAYS_PER_YEAR = 365.25
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 60


def fetch_json(endpoint_path: str, query_parameters: dict) -> dict:
    request_url = f"{GDC_API_URL}/{endpoint_path}?{urllib.parse.urlencode(query_parameters)}"
    with urllib.request.urlopen(request_url, timeout=REQUEST_TIMEOUT_SECONDS) as api_response:
        return json.load(api_response)


def find_smallest_open_slides(project_id: str, slide_count: int) -> list[dict]:
    slide_filters = {
        "op": "and",
        "content": [
            {"op": "=", "content": {"field": "cases.project.project_id", "value": project_id}},
            {"op": "=", "content": {"field": "data_type", "value": "Slide Image"}},
            {"op": "=", "content": {"field": "experimental_strategy", "value": "Diagnostic Slide"}},
            {"op": "=", "content": {"field": "access", "value": "open"}},
        ],
    }
    response_body = fetch_json(
        "files",
        {
            "filters": json.dumps(slide_filters),
            "fields": "file_id,file_name,file_size,cases.case_id,cases.submitter_id",
            "sort": "file_size:asc",
            "size": slide_count,
        },
    )
    return response_body["data"]["hits"]


def build_clinical_record(case_uuid: str, case_submitter_id: str) -> dict:
    case_body = fetch_json(f"cases/{case_uuid}", {"expand": "demographic,diagnoses"})["data"]
    demographic_info = case_body.get("demographic") or {}
    first_diagnosis = (case_body.get("diagnoses") or [{}])[0]
    age_in_days = first_diagnosis.get("age_at_diagnosis")

    return {
        "case_id": case_submitter_id,
        "age": round(age_in_days / DAYS_PER_YEAR) if age_in_days else None,
        "sex": demographic_info.get("sex_at_birth") or demographic_info.get("gender"),
        "diagnosis": first_diagnosis.get("primary_diagnosis"),
        "source": "TCGA via GDC open access",
    }


def download_file(file_id: str, target_path: Path) -> None:
    partial_path = target_path.with_name(target_path.name + ".partial")
    with urllib.request.urlopen(
        f"{GDC_API_URL}/data/{file_id}", timeout=REQUEST_TIMEOUT_SECONDS
    ) as file_response:
        with open(partial_path, "wb") as partial_file:
            shutil.copyfileobj(file_response, partial_file, DOWNLOAD_CHUNK_BYTES)
    partial_path.replace(target_path)


def download_tcga_samples(project_id: str, slide_count: int, output_path: str) -> None:
    output_folder = Path(output_path)
    output_folder.mkdir(parents=True, exist_ok=True)

    for slide_hit in find_smallest_open_slides(project_id, slide_count):
        case_info = slide_hit["cases"][0]
        case_submitter_id = case_info["submitter_id"]
        slide_path = output_folder / f"{case_submitter_id}.svs"
        if slide_path.exists():
            print(f"Skipping {case_submitter_id}, already downloaded")
            continue

        print(f"Downloading {case_submitter_id}: {slide_hit['file_size'] / 1024**2:.0f} MB")
        download_file(slide_hit["file_id"], slide_path)
        clinical_record = build_clinical_record(case_info["case_id"], case_submitter_id)
        (output_folder / f"{case_submitter_id}.json").write_text(json.dumps(clinical_record, indent=2))

    print("Done. TCGA open-access genomics comes as MAF, not VCF, so these cases arrive without genomics.")


def main() -> None:
    argument_parser = argparse.ArgumentParser(
        description="Download open-access TCGA slides with clinical data."
    )
    argument_parser.add_argument("--project-id", default="TCGA-LUAD")
    argument_parser.add_argument("--slide-count", type=int, default=3)
    argument_parser.add_argument("--output-path", default="./sample_data/lab_upload")
    parsed_arguments = argument_parser.parse_args()
    download_tcga_samples(
        parsed_arguments.project_id, parsed_arguments.slide_count, parsed_arguments.output_path
    )


if __name__ == "__main__":
    main()
