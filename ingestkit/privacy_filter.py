import hashlib
import hmac

PATIENT_ID_FIELD = "patient_id"
PSEUDONYM_PREFIX = "P-"
PSEUDONYM_LENGTH = 16


class PrivacyFilter:
    def __init__(self, pseudonym_salt: str, identifier_fields: list[str]):
        self.pseudonym_key = pseudonym_salt.encode("utf-8")
        self.identifier_fields = {field_name.lower() for field_name in identifier_fields}

    def deidentify_record(self, clinical_record: dict) -> dict:
        clean_record = {}
        for field_name, field_value in clinical_record.items():
            if field_name.lower() in self.identifier_fields:
                continue
            clean_record[field_name] = field_value

        if clean_record.get(PATIENT_ID_FIELD):
            clean_record[PATIENT_ID_FIELD] = self.create_pseudonym(str(clean_record[PATIENT_ID_FIELD]))
        return clean_record

    def create_pseudonym(self, real_identifier: str) -> str:
        # HMAC instead of a plain hash: without the secret key nobody can rebuild the mapping
        # by hashing a list of known hospital IDs.
        identifier_digest = hmac.new(self.pseudonym_key, real_identifier.encode("utf-8"), hashlib.sha256)
        return PSEUDONYM_PREFIX + identifier_digest.hexdigest()[:PSEUDONYM_LENGTH]
