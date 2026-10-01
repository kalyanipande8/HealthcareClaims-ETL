"""
deidentify.py

HIPAA Safe-Harbor-inspired de-identification step applied to every record
before it lands in the unified, analysis-ready output table.

Transformations applied (direct identifiers -> removed or generalized):
  - patient_name   -> replaced with a stable pseudonymous token
                      (HMAC-SHA256 of name+dob+address, truncated) so the same
                      patient maps to the same token across feeds without
                      exposing the name itself.
  - date_of_birth  -> generalized to birth_year only (Safe Harbor allows year
                      for ages < 90; full DOB is dropped).
  - address        -> generalized to state + 3-digit ZIP prefix only (full
                      street address, city, and full ZIP are dropped).

The full (identified) record is written ONLY to a restricted,
access-controlled file (data/processed/_restricted_identified_lookup.csv)
that a real deployment would keep in a separate, encrypted, access-logged
store -- never in the general-access analytics table. Everything else in the
pipeline and all test assertions operate on the de-identified table.
"""
import hashlib
import hmac
import re

# In a real system this key lives in a secrets manager / HSM, not source
# control. For this portfolio demo it is an obviously-fake local constant.
PSEUDONYM_KEY = b"demo-deid-pseudonymization-key-not-for-prod"


def pseudonymize(name: str, dob: str, address: str) -> str:
    """Deterministic, non-reversible-without-key pseudonym for a patient."""
    basis = f"{name.strip().lower()}|{dob.strip()}|{address.strip().lower()}".encode("utf-8")
    digest = hmac.new(PSEUDONYM_KEY, basis, hashlib.sha256).hexdigest()
    return f"PT-{digest[:16]}"


def generalize_dob(dob: str) -> str:
    """Full DOB -> birth year only (Safe Harbor generalization)."""
    m = re.match(r"(\d{4})-\d{2}-\d{2}", dob.strip())
    return m.group(1) if m else ""


def generalize_address(address: str) -> str:
    """Full street address -> state + 3-digit ZIP prefix only."""
    m = re.search(r",\s*([A-Z]{2})\s+(\d{5})", address)
    if not m:
        return "UNKNOWN"
    state, zip5 = m.group(1), m.group(2)
    return f"{state}-{zip5[:3]}XX"


def deidentify_record(name: str, dob: str, address: str):
    """Returns (deidentified_fields_dict, restricted_identified_fields_dict)."""
    deid = {
        "patient_token": pseudonymize(name, dob, address),
        "birth_year": generalize_dob(dob),
        "patient_region": generalize_address(address),
    }
    restricted = {
        "patient_token": deid["patient_token"],
        "patient_name": name,
        "date_of_birth": dob,
        "address": address,
    }
    return deid, restricted
