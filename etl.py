"""
etl.py

Python ETL framework that consolidates authenticated claims + pharmaceutical
supply feeds from 5 distinct suppliers (5 distinct file formats / schemas)
into ONE unified schema, with automated data-quality / completeness checks
and a HIPAA-aware de-identification step, plus an audit log of every run.

Unified schema (one row per record, regardless of source feed format):

    record_id, record_type, supplier, event_date, code, amount_or_quantity,
    patient_token, birth_year, patient_region, source_file, dq_flags

`record_type` is "claim" (code = diagnosis code, amount_or_quantity = billed
amount) or "supply" (code = NDC code, amount_or_quantity = quantity shipped).
Supply records have no patient fields (patient_token/birth_year/
patient_region are blank) since pharma supply shipments are not tied to an
individual patient in these feeds.
"""
import csv
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from deidentify import deidentify_record
from feed_auth import authenticate_all

ROOT = Path(__file__).parent
PROCESSED = ROOT / "data" / "processed"
LOGS = ROOT / "logs"
PROCESSED.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

UNIFIED_FIELDS = [
    "record_id", "record_type", "supplier", "event_date", "code",
    "amount_or_quantity", "patient_token", "birth_year", "patient_region",
    "source_file", "dq_flags",
]

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# Per-supplier parsers -- each returns a list of dicts with *raw* fields;
# raw PHI fields (name/dob/address) are consumed immediately by
# deidentify_record() inside _to_unified() and never persisted raw.
# ---------------------------------------------------------------------------
def parse_medplus_claims(path: Path):
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "record_type": "claim",
                "raw_id": r["claim_id"],
                "name": r["patient_name"],
                "dob": r["dob"],
                "address": r["address"],
                "code": r["diagnosis_code"],
                "amount": r["billed_amount"],
                "event_date": r["service_date"],
            })
    return rows


def parse_pharmacorp_supply(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for s in data["shipments"]:
        rows.append({
            "record_type": "supply",
            "raw_id": s["shipment_id"],
            "name": None, "dob": None, "address": None,
            "code": s["ndc_code"],
            "amount": s["quantity_units"],
            "event_date": s["ship_date"],
        })
    return rows


def parse_regionalhealth_claims(path: Path):
    rows = []
    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("|")
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) != len(header):
            # malformed row (e.g. the injected tamper row) -- skip, it will
            # never reach here anyway since the whole file fails auth, but
            # defend in depth regardless.
            continue
        rec = dict(zip(header, parts))
        rows.append({
            "record_type": "claim",
            "raw_id": rec["CLAIMID"],
            "name": rec["PATIENTNAME"],
            "dob": rec["DOB"],
            "address": rec["ADDR"],
            "code": rec["DXCODE"],
            "amount": rec["AMT"],
            "event_date": rec["SVCDATE"],
        })
    return rows


def parse_vitalrx_supply(path: Path):
    tree = ET.parse(path)
    rows = []
    for s in tree.getroot().findall("shipment"):
        rows.append({
            "record_type": "supply",
            "raw_id": s.findtext("id"),
            "name": None, "dob": None, "address": None,
            "code": s.findtext("ndc"),
            "amount": s.findtext("qty"),
            "event_date": s.findtext("shipped"),
        })
    return rows


def parse_summitcare_claims(path: Path):
    # Fixed-width: claim_id(10) name(24) dob(10) dx(8) amount(10)
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        claim_id = line[0:10].strip()
        name = line[10:34].strip()
        dob = line[34:44].strip()
        dx = line[44:52].strip()
        amount = line[52:62].strip()
        rows.append({
            "record_type": "claim",
            "raw_id": claim_id,
            "name": name,
            "dob": dob,
            "address": "UNKNOWN, NY 00000",  # fixed-width feed has no address field
            "code": dx,
            "amount": amount,
            "event_date": None,
        })
    return rows


PARSERS = {
    "medplus_claims": parse_medplus_claims,
    "pharmacorp_supply": parse_pharmacorp_supply,
    "regionalhealth_claims": parse_regionalhealth_claims,
    "vitalrx_supply": parse_vitalrx_supply,
    "summitcare_claims": parse_summitcare_claims,
}


# ---------------------------------------------------------------------------
# Unified transform + data-quality checks
# ---------------------------------------------------------------------------
def _to_unified(raw_rows, supplier, source_file):
    unified = []
    for r in raw_rows:
        flags = []

        if r["name"]:
            deid, restricted = deidentify_record(r["name"], r["dob"] or "", r["address"] or "")
        else:
            deid = {"patient_token": "", "birth_year": "", "patient_region": ""}
            restricted = None

        event_date = r.get("event_date") or ""
        if not event_date:
            flags.append("missing_event_date")
        elif not DATE_RE.match(event_date):
            flags.append("malformed_event_date")

        amount_raw = r.get("amount")
        try:
            amount = float(amount_raw)
        except (TypeError, ValueError):
            amount = None
            flags.append("missing_or_invalid_amount")
        else:
            if amount <= 0:
                flags.append("non_positive_amount")
            if amount > 20000:
                flags.append("amount_outlier_high")

        code = (r.get("code") or "").strip()
        if not code:
            flags.append("missing_code")

        if not r.get("raw_id"):
            flags.append("missing_record_id")

        unified.append(({
            "record_id": r.get("raw_id") or "",
            "record_type": r["record_type"],
            "supplier": supplier,
            "event_date": event_date,
            "code": code,
            "amount_or_quantity": amount if amount is not None else "",
            "patient_token": deid["patient_token"],
            "birth_year": deid["birth_year"],
            "patient_region": deid["patient_region"],
            "source_file": source_file,
            "dq_flags": ";".join(flags),
        }, restricted))
    return unified


def run_etl():
    """Authenticates all incoming feeds, parses the authenticated ones,
    de-identifies PHI, runs data-quality checks, and writes:
      - data/processed/unified_records.csv        (de-identified, general access)
      - data/processed/_restricted_identified_lookup.csv  (identified, restricted)
      - data/processed/dq_summary.json             (completeness / quality metrics)
      - logs/audit_log.csv                         (append-only run audit trail)
    Returns a summary dict (also used by tests).
    """
    run_started = datetime.now(timezone.utc)
    auth_results = authenticate_all()

    unified_rows = []
    restricted_rows = []
    per_supplier_counts = {}
    rejected_files = []

    for ar in auth_results:
        if not ar.authenticated:
            rejected_files.append({"file": ar.path.name, "supplier": ar.supplier, "reason": ar.reason})
            continue
        parser = PARSERS.get(ar.supplier)
        if parser is None:
            continue
        raw_rows = parser(ar.path)
        pairs = _to_unified(raw_rows, ar.supplier, ar.path.name)
        for row, restricted in pairs:
            unified_rows.append(row)
            if restricted:
                restricted_rows.append(restricted)
        per_supplier_counts[ar.supplier] = per_supplier_counts.get(ar.supplier, 0) + len(raw_rows)

    # --- write unified (de-identified) table ---
    unified_path = PROCESSED / "unified_records.csv"
    with unified_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=UNIFIED_FIELDS)
        w.writeheader()
        w.writerows(unified_rows)

    # --- write restricted identified lookup (access-controlled in a real deployment) ---
    restricted_path = PROCESSED / "_restricted_identified_lookup.csv"
    with restricted_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["patient_token", "patient_name", "date_of_birth", "address"])
        w.writeheader()
        w.writerows(restricted_rows)

    # --- data quality / completeness summary ---
    total = len(unified_rows)
    flagged = sum(1 for r in unified_rows if r["dq_flags"])
    flag_counts = {}
    for r in unified_rows:
        for flag in filter(None, r["dq_flags"].split(";")):
            flag_counts[flag] = flag_counts.get(flag, 0) + 1

    completeness_fields = ["record_id", "event_date", "code", "amount_or_quantity"]
    completeness = {}
    for field_name in completeness_fields:
        non_empty = sum(1 for r in unified_rows if str(r[field_name]) != "")
        completeness[field_name] = round(non_empty / total, 4) if total else 0.0

    dq_summary = {
        "run_timestamp_utc": run_started.isoformat(),
        "feeds_seen": len(auth_results),
        "feeds_authenticated": sum(1 for a in auth_results if a.authenticated),
        "feeds_rejected": rejected_files,
        "suppliers_loaded": sorted(per_supplier_counts.keys()),
        "supplier_record_counts": per_supplier_counts,
        "total_unified_records": total,
        "records_with_dq_flags": flagged,
        "dq_flag_counts": flag_counts,
        "field_completeness": completeness,
        "deidentified_patient_records": sum(1 for r in restricted_rows),
    }
    (PROCESSED / "dq_summary.json").write_text(json.dumps(dq_summary, indent=2))

    # --- audit log (append-only) ---
    audit_path = LOGS / "audit_log.csv"
    is_new = not audit_path.exists()
    with audit_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if is_new:
            w.writerow(["timestamp_utc", "actor", "action", "feeds_authenticated",
                        "feeds_rejected", "records_loaded", "records_flagged"])
        w.writerow([
            run_started.isoformat(), "etl_pipeline_service", "run_etl",
            dq_summary["feeds_authenticated"], len(rejected_files), total, flagged,
        ])

    return dq_summary


if __name__ == "__main__":
    summary = run_etl()
    print(json.dumps(summary, indent=2))
