import csv
import io
import json
from typing import BinaryIO

from ingestkit.pipeline_errors import QuarantineError

MAX_CLINICAL_BYTES = 5 * 1024 * 1024
SEX_VALUES = {"f": "F", "female": "F", "m": "M", "male": "M"}
MAX_AGE_YEARS = 120


class ClinicalReader:
    def __init__(self, required_fields: list[str]):
        self.required_fields = required_fields

    def read_clinical_record(self, clinical_stream: BinaryIO, file_extension: str, case_id: str) -> dict:
        clinical_bytes = clinical_stream.read(MAX_CLINICAL_BYTES + 1)
        if len(clinical_bytes) > MAX_CLINICAL_BYTES:
            raise QuarantineError("Clinical file is larger than 5 MB, expected one patient record")

        try:
            clinical_text = clinical_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            raise QuarantineError(f"Clinical file is not UTF-8 text: {e}") from e

        if file_extension == ".json":
            clinical_record = self._parse_json_record(clinical_text)
        else:
            clinical_record = self._parse_csv_record(clinical_text)

        self._check_required_fields(clinical_record)
        self._check_case_id(clinical_record, case_id)
        return self._normalise_record(clinical_record)

    @staticmethod
    def _parse_json_record(clinical_text: str) -> dict:
        try:
            parsed_value = json.loads(clinical_text)
        except json.JSONDecodeError as e:
            raise QuarantineError(f"Clinical JSON is invalid: {e}") from e

        if isinstance(parsed_value, list) and len(parsed_value) == 1:
            parsed_value = parsed_value[0]
        if not isinstance(parsed_value, dict):
            raise QuarantineError("Clinical JSON must hold one patient record")
        return parsed_value

    @staticmethod
    def _parse_csv_record(clinical_text: str) -> dict:
        csv_rows = list(csv.DictReader(io.StringIO(clinical_text)))
        if len(csv_rows) != 1:
            raise QuarantineError(f"Clinical CSV must hold exactly one patient row, found {len(csv_rows)}")
        return csv_rows[0]

    def _check_required_fields(self, clinical_record: dict) -> None:
        missing_fields = [
            field_name for field_name in self.required_fields if clinical_record.get(field_name) in (None, "")
        ]
        if missing_fields:
            raise QuarantineError(f"Clinical record is missing required fields: {', '.join(missing_fields)}")

    @staticmethod
    def _check_case_id(clinical_record: dict, case_id: str) -> None:
        record_case_id = str(clinical_record.get("case_id", "")).strip()
        if record_case_id and record_case_id != case_id:
            raise QuarantineError(f"case_id in file ({record_case_id}) does not match file name ({case_id})")

    @staticmethod
    def _normalise_record(clinical_record: dict) -> dict:
        normalised_record = dict(clinical_record)

        if "age" in normalised_record:
            try:
                patient_age = int(normalised_record["age"])
            except (TypeError, ValueError) as e:
                raise QuarantineError(
                    f"Clinical age is not a whole number: {normalised_record['age']}"
                ) from e
            if not 0 <= patient_age <= MAX_AGE_YEARS:
                raise QuarantineError(f"Clinical age is out of range: {patient_age}")
            normalised_record["age"] = patient_age

        if "sex" in normalised_record:
            sex_text = str(normalised_record["sex"]).strip().lower()
            normalised_record["sex"] = SEX_VALUES.get(sex_text, "unknown")

        if "diagnosis" in normalised_record:
            normalised_record["diagnosis"] = str(normalised_record["diagnosis"]).strip()

        return normalised_record
