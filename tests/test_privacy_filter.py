from ingestkit.privacy_filter import PrivacyFilter

IDENTIFIER_FIELDS = ["name", "birth_date", "address"]


def test_identifier_fields_are_removed():
    privacy_filter = PrivacyFilter("salt-for-tests", IDENTIFIER_FIELDS)
    clean_record = privacy_filter.deidentify_record(
        {"case_id": "C-0042", "Name": "Jane Example", "birth_date": "1955-02-01", "age": 71}
    )
    assert clean_record == {"case_id": "C-0042", "age": 71}


def test_patient_id_is_replaced_with_stable_pseudonym():
    privacy_filter = PrivacyFilter("salt-for-tests", IDENTIFIER_FIELDS)
    first_record = privacy_filter.deidentify_record({"patient_id": "HOSP-123"})
    second_record = privacy_filter.deidentify_record({"patient_id": "HOSP-123"})

    assert first_record["patient_id"] == second_record["patient_id"]
    assert first_record["patient_id"].startswith("P-")
    assert "HOSP-123" not in first_record["patient_id"]


def test_different_salt_gives_different_pseudonym():
    first_filter = PrivacyFilter("salt-for-tests", IDENTIFIER_FIELDS)
    second_filter = PrivacyFilter("another-secret-salt", IDENTIFIER_FIELDS)
    assert first_filter.create_pseudonym("HOSP-123") != second_filter.create_pseudonym("HOSP-123")
