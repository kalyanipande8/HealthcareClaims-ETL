# HealthcareClaims-ETL: Multi-Supplier Claims & Pharma Feed Authentication + ETL

A Python ETL framework that authenticates, validates, and consolidates
healthcare claims and pharmaceutical supply data arriving from **5 distinct
suppliers**, each using a genuinely different feed format and schema (CSV,
JSON, pipe-delimited, XML, fixed-width) — the way real clearinghouse /
trading-partner integrations actually look. Includes a real feed-level
authentication boundary (HMAC-SHA256 signatures, with a deliberately
corrupted feed that correctly fails), a HIPAA Safe-Harbor-style
de-identification step, automated data-quality/completeness checks, and
substantive HIPAA compliance documentation.

> **Disclaimer:** This is a portfolio/demo project. All supplier feeds,
> claims, and patient data (name, DOB, address) are synthetically generated
> by `simulate_supplier_feeds.py`. No real patient, claim, employer, or
> supplier data is used anywhere in this repository.

## How this differs from `HealthcareETL-Warehouse`

The sibling project `HealthcareETL-Warehouse` is a general-purpose
healthcare data warehouse build (source extraction -> transform ->
warehouse schema -> data-model deliverable). **This project is narrower and
deliberately different in focus:**

- Scoped specifically to **claims + pharmaceutical supplier data**, not a
  general healthcare data model.
- Centers on **multi-supplier feed authentication/validation** — a real
  cryptographic signature-verification boundary (HMAC-SHA256) with a
  negative test case (a corrupted feed that must fail closed), not just
  parsing/loading trusted files.
- Centers on **HIPAA compliance documentation** as a first-class deliverable
  (`docs/hipaa_compliance_documentation.md`), covering de-identification
  approach, access controls, and audit logging — rather than warehouse
  schema design.
- Output is a flat, analysis-ready **unified table** for claims + supply
  records, not a dimensional warehouse model.

## Pipeline

1. **`simulate_supplier_feeds.py`** — generates synthetic claims/pharma-supply
   feeds for 5 suppliers in 5 different formats, each signed with a
   per-supplier HMAC-SHA256 key:
   - **MedPlus Claims** — CSV
   - **PharmaCorp Supply** — JSON
   - **RegionalHealth Claims** — pipe-delimited flat file (2 batches; batch 2
     is tampered *after* signing to simulate corruption/in-transit alteration)
   - **VitalRx Supply** — XML
   - **SummitCare Claims** — fixed-width text

2. **`feed_auth.py`** — authenticates every inbound file by recomputing its
   HMAC-SHA256 signature and comparing (constant-time) to the supplier's
   signed `.sig` sidecar. Anything unsigned, signed by an unknown supplier,
   or mismatched (tampered) is quarantined to `data/rejected/` and never
   parsed.

3. **`deidentify.py`** — applies a Safe-Harbor-style transform to every claim
   record: name -> keyed pseudonym token, DOB -> birth year only, address ->
   state + 3-digit ZIP prefix. Full identified records are written *only* to
   a separate restricted file, never into the general-access output.

4. **`etl.py`** — parses each authenticated feed with its own
   format-specific parser, de-identifies PHI, runs automated data-quality
   checks (missing/malformed dates, missing/invalid amounts, outlier
   amounts, missing codes/IDs), consolidates everything into one unified
   schema, and writes an append-only audit log of every run.

5. **`run_all.py`** runs the whole pipeline end-to-end.

## How to run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python run_all.py
pytest -q
```

## Results

_All numbers below are copied directly from an actual run of `python
run_all.py` and `pytest -q` — nothing is hardcoded. Fixed random seed (42)
makes the feed simulation reproducible._

- **6 feed files from 5 suppliers** submitted; **5 authenticated, 1 rejected**
  — the deliberately-corrupted `regionalhealth_claims_batch2_corrupted.txt`
  fails HMAC verification (`signature mismatch`) and is quarantined to
  `data/rejected/` without ever being parsed.
- **160 unified records** consolidated from the 5 authenticated feeds
  (40 MedPlus + 30 PharmaCorp + 35 RegionalHealth + 30 SummitCare + 25 VitalRx).
- **105 claim records de-identified** — patient name/DOB/address replaced
  with a pseudonymous token, birth year, and region in the general-access
  output; full identified data isolated to
  `data/processed/_restricted_identified_lookup.csv`.
- **Field completeness:** `record_id` 100%, `code` 100%, `amount_or_quantity`
  100%, `event_date` 81.25% (SummitCare's fixed-width feed has no service-date
  field, correctly flagged as `missing_event_date` by the data-quality check
  rather than silently dropped).
- **30 of 160 records (18.75%) flagged** with at least one data-quality issue
  (all `missing_event_date`, from the one supplier whose format lacks that
  field) — visible per-record in `dq_flags` and aggregated in
  `data/processed/dq_summary.json`.
- **`pytest -q` → 11 passed** in 0.05s, including:
  - at least 4 distinct suppliers simulated and present in the unified output
  - the corrupted feed fails authentication and is quarantined
  - no corrupted/injected content ever reaches the unified output
  - de-identification actually removes PHI (no synthetic patient names,
    street names, or full DOBs found anywhere in `unified_records.csv`)
  - pseudonymization is deterministic per patient (same inputs -> same token)
  - the restricted identified lookup is a separate file from the
    general-access unified table

## Outputs

- `data/processed/unified_records.csv` — de-identified, analysis-ready table
- `data/processed/_restricted_identified_lookup.csv` — identified PHI,
  isolated (represents an access-controlled store in a real deployment)
- `data/processed/dq_summary.json` — per-run data-quality/completeness metrics
- `data/rejected/` — quarantined feeds that failed authentication
- `logs/audit_log.csv` — append-only audit trail of every pipeline run

## Compliance documentation

See [`docs/hipaa_compliance_documentation.md`](docs/hipaa_compliance_documentation.md)
for the de-identification approach, access-control pattern, and audit
logging implemented in this pipeline.
