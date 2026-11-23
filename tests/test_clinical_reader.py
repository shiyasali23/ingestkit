import io

import pytest

from ingestkit.clinical_reader import ClinicalReader
from ingestkit.pipeline_errors import QuarantineError
from tests.conftest import build_clinical_json

REQUIRED_FIELDS = ["case_id", "age", "sex", "diagnosis"]


def read_record(clinical_bytes: bytes, file_extension: str = ".json", case_id: str = "C-0042") -> dict:
    clinical_reader = ClinicalReader(REQUIRED_FIELDS)
    return clinical_reader.read_clinical_record(io.BytesIO(clinical_bytes), file_extension, case_id)


def test_json_record_is_normalised():
    clinical_record = read_record(build_clinical_json("C-0042", sex="Female", age="64"))
    assert clinical_record["sex"] == "F"
    assert clinical_record["age"] == 64
    assert clinical_record["diagnosis"] == "MDS"


def test_csv_record_is_read():
    csv_bytes = b"case_id,age,sex,diagnosis\nC-0042,58,M,CLL\n"
    clinical_record = read_record(csv_bytes, file_extension=".csv")
    assert clinical_record == {"case_id": "C-0042", "age": 58, "sex": "M", "diagnosis": "CLL"}


def test_missing_required_field_is_quarantined():
    with pytest.raises(QuarantineError, match="diagnosis"):
        read_record(b'{"case_id": "C-0042", "age": 70, "sex": "F"}')


def test_case_id_must_match_file_name():
    with pytest.raises(QuarantineError, match="does not match"):
        read_record(build_clinical_json("C-9999"))


def test_impossible_age_is_quarantined():
    with pytest.raises(QuarantineError, match="out of range"):
        read_record(build_clinical_json("C-0042", age=150))


def test_broken_json_is_quarantined():
    with pytest.raises(QuarantineError, match="invalid"):
        read_record(b'{"case_id": "C-0042",')


def test_csv_with_two_patients_is_quarantined():
    csv_bytes = b"case_id,age,sex,diagnosis\nC-0042,58,M,CLL\nC-0042,60,F,MDS\n"
    with pytest.raises(QuarantineError, match="exactly one"):
        read_record(csv_bytes, file_extension=".csv")
