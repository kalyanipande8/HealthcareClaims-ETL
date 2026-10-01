import csv
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import pytest

import simulate_supplier_feeds
import feed_auth
import etl


@pytest.fixture(scope="module", autouse=True)
def fresh_pipeline_run():
    """Simulate feeds and run the full pipeline once for this test module."""
    for d in ["data/incoming", "data/rejected", "data/processed"]:
        p = ROOT / d
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)
    simulate_supplier_feeds.main()
    summary = etl.run_etl()
    yield summary


def test_at_least_four_distinct_suppliers_simulated():
    suppliers = set(simulate_supplier_feeds.SUPPLIER_KEYS.keys())
    assert len(suppliers) >= 4
    # and each supplier actually produced a feed file in data/incoming/
    incoming_files = [p.name for p in (ROOT / "data" / "incoming").glob("*") if not p.name.endswith(".sig")]
    for supplier_prefix in feed_auth.SUPPLIER_PREFIXES:
        assert any(f.startswith(supplier_prefix) for f in incoming_files), \
            f"no feed file found for supplier {supplier_prefix}"


def test_corrupted_feed_fails_authentication_and_is_quarantined():
    results = feed_auth.authenticate_all()
    corrupted = [r for r in results if "corrupted" in r.path.name]
    assert len(corrupted) == 1, "expected exactly one deliberately-corrupted feed file"
    assert corrupted[0].authenticated is False
    assert "mismatch" in corrupted[0].reason

    # and it must have been quarantined, not loaded
    rejected_names = [p.name for p in (ROOT / "data" / "rejected").glob("*") if not p.name.endswith(".sig")]
    assert any("corrupted" in n for n in rejected_names)


def test_valid_feeds_pass_authentication():
    results = feed_auth.authenticate_all()
    good = [r for r in results if "corrupted" not in r.path.name]
    assert len(good) >= 5
    assert all(r.authenticated for r in good), \
        [(r.path.name, r.reason) for r in good if not r.authenticated]


def test_unified_output_has_records_from_all_authenticated_suppliers(fresh_pipeline_run):
    summary = fresh_pipeline_run
    assert summary["feeds_authenticated"] >= 5
    assert summary["total_unified_records"] > 0
    assert len(summary["suppliers_loaded"]) >= 4

    unified_path = ROOT / "data" / "processed" / "unified_records.csv"
    with unified_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    suppliers_in_output = {r["supplier"] for r in rows}
    assert len(suppliers_in_output) >= 4
    # corrupted supplier's regionalhealth_claims batch1 (valid) should still be present
    assert "regionalhealth_claims" in suppliers_in_output


def test_corrupted_records_never_reach_unified_output():
    unified_path = ROOT / "data" / "processed" / "unified_records.csv"
    with unified_path.open(newline="", encoding="utf-8") as f:
        content = f.read()
    assert "INJECTED" not in content
    assert "???" not in content


def test_deidentification_removes_phi_from_unified_output():
    unified_path = ROOT / "data" / "processed" / "unified_records.csv"
    content = unified_path.read_text(encoding="utf-8")

    # None of the synthetic first/last names used to generate PHI should
    # appear anywhere in the de-identified unified output.
    for name in simulate_supplier_feeds.FIRST_NAMES + simulate_supplier_feeds.LAST_NAMES:
        assert name not in content, f"PHI leak: name fragment '{name}' found in de-identified output"

    # No full street addresses (digits + street words) should appear.
    for street in simulate_supplier_feeds.STREETS:
        assert street not in content, f"PHI leak: street '{street}' found in de-identified output"

    # Full DOB (YYYY-MM-DD) must not appear for patient rows -- only birth_year.
    import re
    with unified_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    claim_rows = [r for r in rows if r["record_type"] == "claim"]
    assert claim_rows, "expected at least one claim record"
    for r in claim_rows:
        assert re.match(r"^\d{4}$", r["birth_year"]), f"birth_year not generalized: {r['birth_year']}"
        assert r["patient_token"].startswith("PT-")
        assert r["patient_region"] != ""


def test_deidentification_is_deterministic_per_patient():
    from deidentify import pseudonymize
    t1 = pseudonymize("Jane Doe", "1980-01-01", "123 Main St, Buffalo, NY 14201")
    t2 = pseudonymize("Jane Doe", "1980-01-01", "123 Main St, Buffalo, NY 14201")
    t3 = pseudonymize("Jane Doe", "1980-01-02", "123 Main St, Buffalo, NY 14201")
    assert t1 == t2
    assert t1 != t3


def test_restricted_identified_lookup_is_separate_from_general_output():
    restricted_path = ROOT / "data" / "processed" / "_restricted_identified_lookup.csv"
    assert restricted_path.exists()
    with restricted_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) > 0
    assert "patient_name" in rows[0]
    unified_path = ROOT / "data" / "processed" / "unified_records.csv"
    with unified_path.open(newline="", encoding="utf-8") as f:
        unified_fields = csv.DictReader(f).fieldnames
    assert "patient_name" not in unified_fields
    assert "date_of_birth" not in unified_fields
    assert "address" not in unified_fields


def test_data_quality_checks_flag_bad_records(fresh_pipeline_run):
    summary = fresh_pipeline_run
    assert "field_completeness" in summary
    for field_name, pct in summary["field_completeness"].items():
        assert 0.0 <= pct <= 1.0
    # completeness should be meaningfully high since feeds are mostly well-formed
    assert summary["field_completeness"]["record_id"] > 0.9


def test_audit_log_written():
    audit_path = ROOT / "logs" / "audit_log.csv"
    assert audit_path.exists()
    with audit_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) >= 1
    assert rows[-1]["action"] == "run_etl"


def test_dq_summary_json_written_and_well_formed():
    dq_path = ROOT / "data" / "processed" / "dq_summary.json"
    assert dq_path.exists()
    summary = json.loads(dq_path.read_text())
    assert summary["feeds_authenticated"] >= 5
    assert isinstance(summary["dq_flag_counts"], dict)
