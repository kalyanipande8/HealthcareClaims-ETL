"""run_all.py -- runs the full pipeline end-to-end and prints a results summary."""
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).parent


def main():
    # Fresh run: clear previously simulated/processed artifacts so results are
    # reproducible from a clean state each time.
    for d in ["data/incoming", "data/rejected", "data/processed"]:
        p = ROOT / d
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STEP 1/3: Simulating supplier feeds (5 suppliers, 6 files)")
    print("=" * 70)
    import simulate_supplier_feeds
    simulate_supplier_feeds.main()

    print()
    print("=" * 70)
    print("STEP 2/3: Authenticating inbound feeds (HMAC-SHA256 signature check)")
    print("=" * 70)
    import feed_auth
    feed_auth.main()

    print()
    print("=" * 70)
    print("STEP 3/3: Running ETL -- parse, de-identify, quality-check, consolidate")
    print("=" * 70)
    import etl
    summary = etl.run_etl()
    print(json.dumps(summary, indent=2))

    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Feeds seen:            {summary['feeds_seen']}")
    print(f"Feeds authenticated:   {summary['feeds_authenticated']}")
    print(f"Feeds rejected:        {len(summary['feeds_rejected'])} "
          f"({[r['file'] for r in summary['feeds_rejected']]})")
    print(f"Suppliers loaded:      {summary['suppliers_loaded']}")
    print(f"Unified records:       {summary['total_unified_records']}")
    print(f"Records with DQ flags: {summary['records_with_dq_flags']}")
    print(f"De-identified patients:{summary['deidentified_patient_records']}")
    print(f"Field completeness:    {summary['field_completeness']}")
    print()
    print("Outputs written to data/processed/ and logs/audit_log.csv")


if __name__ == "__main__":
    main()
