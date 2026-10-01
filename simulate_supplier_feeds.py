"""
simulate_supplier_feeds.py

Simulates inbound data feeds from 5 distinct healthcare claims / pharmaceutical
supply "suppliers," each using a genuinely different file format and schema,
the way real trading partners actually send data. Every feed is signed with an
HMAC-SHA256 signature (per-supplier shared secret), the way a real feed
ingestion boundary authenticates inbound files before anything downstream
touches them.

One feed (RegionalHealth Claims, batch 2) is deliberately corrupted AFTER
signing, so its signature no longer matches its content -- this is the
negative case that `feed_auth.py` / the tests must correctly reject.

All patient data (name, DOB, address) is synthetic, generated with Faker-style
hand-rolled name/address pools. No real patient, claim, or supplier data is
used anywhere in this project.
"""
import csv
import hashlib
import hmac
import json
import random
from pathlib import Path

RNG_SEED = 42
random.seed(RNG_SEED)

ROOT = Path(__file__).parent
INCOMING = ROOT / "data" / "incoming"
INCOMING.mkdir(parents=True, exist_ok=True)

# Per-supplier shared secret keys (simulating keys exchanged out-of-band /
# stored in a secrets manager in a real integration).
SUPPLIER_KEYS = {
    "medplus_claims": b"medplus-shared-secret-2024",
    "pharmacorp_supply": b"pharmacorp-hmac-key-9f21",
    "regionalhealth_claims": b"regionalhealth-key-aa17",
    "vitalrx_supply": b"vitalrx-signing-secret-55",
    "summitcare_claims": b"summitcare-key-7731",
}

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
               "Linda", "William", "Elizabeth", "David", "Barbara", "Carlos", "Maria",
               "Wei", "Fatima", "Aisha", "Deepak", "Svetlana", "Kofi"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Chen", "Patel", "Nguyen", "Kim",
              "Okafor", "Ivanov", "Silva", "Kowalski", "Haddad", "Singh"]
STREETS = ["Maple St", "Oak Ave", "Main St", "Cedar Ln", "Park Dr", "Elm St",
           "Sunset Blvd", "River Rd", "Hillcrest Ave", "Lakeview Dr"]
CITIES_STATES = [("Buffalo", "NY", "14201"), ("Rochester", "NY", "14604"),
                  ("Albany", "NY", "12207"), ("Syracuse", "NY", "13202"),
                  ("Yonkers", "NY", "10701"), ("New Rochelle", "NY", "10801")]
DIAGNOSIS_CODES = ["E11.9", "I10", "J45.909", "M54.5", "K21.9", "F41.1", "N39.0", "E78.5"]
DRUG_NAMES = [("Atorvastatin", "00071015523"), ("Metformin", "00093715301"),
              ("Lisinopril", "00185014501"), ("Albuterol", "00173068220"),
              ("Omeprazole", "00904198061"), ("Amlodipine", "68180051603"),
              ("Sertraline", "00049497066"), ("Insulin Glargine", "00088221005")]


def _random_person():
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    year = random.randint(1940, 2005)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    city, state, zipc = random.choice(CITIES_STATES)
    addr = f"{random.randint(100,9999)} {random.choice(STREETS)}, {city}, {state} {zipc}"
    return f"{fn} {ln}", f"{year:04d}-{month:02d}-{day:02d}", addr


def _sign(payload: bytes, key: bytes) -> str:
    return hmac.new(key, payload, hashlib.sha256).hexdigest()


def _write_signed(supplier_key: str, filename: str, content: str):
    """Write a feed file plus its .sig sidecar file (HMAC-SHA256 of the raw bytes)."""
    path = INCOMING / filename
    data = content.encode("utf-8")
    path.write_bytes(data)
    sig = _sign(data, SUPPLIER_KEYS[supplier_key])
    (INCOMING / f"{filename}.sig").write_text(sig)
    return path


# ---------------------------------------------------------------------------
# Supplier 1: MedPlus Claims -- CSV
# ---------------------------------------------------------------------------
def build_medplus_claims(n=40):
    rows = [["claim_id", "patient_name", "dob", "address", "diagnosis_code",
             "billed_amount", "service_date"]]
    for i in range(n):
        name, dob, addr = _random_person()
        rows.append([
            f"MP-{10000+i}", name, dob, addr,
            random.choice(DIAGNOSIS_CODES),
            f"{random.uniform(80, 5200):.2f}",
            f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        ])
    buf = []
    for r in rows:
        buf.append(",".join(f'"{c}"' for c in r))
    content = "\n".join(buf) + "\n"
    return _write_signed("medplus_claims", "medplus_claims_20240601.csv", content)


# ---------------------------------------------------------------------------
# Supplier 2: PharmaCorp Supply -- JSON
# ---------------------------------------------------------------------------
def build_pharmacorp_supply(n=30):
    records = []
    for i in range(n):
        drug, ndc = random.choice(DRUG_NAMES)
        records.append({
            "shipment_id": f"PC-SHIP-{5000+i}",
            "drug_name": drug,
            "ndc_code": ndc,
            "lot_number": f"LOT{random.randint(1000,9999)}",
            "quantity_units": random.randint(50, 5000),
            "ship_date": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "destination_pharmacy": f"Pharmacy #{random.randint(1,99)}",
        })
    content = json.dumps({"supplier": "PharmaCorp", "shipments": records}, indent=2)
    return _write_signed("pharmacorp_supply", "pharmacorp_supply_20240601.json", content)


# ---------------------------------------------------------------------------
# Supplier 3: RegionalHealth Claims -- pipe-delimited flat file
# ---------------------------------------------------------------------------
def build_regionalhealth_claims(n=35, corrupt=False):
    header = "CLAIMID|PATIENTNAME|DOB|ADDR|DXCODE|AMT|SVCDATE"
    lines = [header]
    for i in range(n):
        name, dob, addr = _random_person()
        amt = round(random.uniform(60, 4800), 2)
        if corrupt and i == 0:
            amt = -999999  # obviously corrupted value, but corruption is really
            # about the byte-level tamper applied below after signing
        lines.append("|".join([
            f"RH-{20000+i}", name, dob, addr.replace("|", ""),
            random.choice(DIAGNOSIS_CODES), f"{amt:.2f}",
            f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        ]))
    content = "\n".join(lines) + "\n"
    filename = "regionalhealth_claims_batch2_corrupted.txt" if corrupt \
        else "regionalhealth_claims_batch1.txt"
    path = _write_signed("regionalhealth_claims", filename, content)
    if corrupt:
        # Tamper with the file AFTER it has been signed, simulating data
        # corrupted/altered in transit or by a man-in-the-middle after the
        # supplier's signature was already computed. The .sig file on disk
        # now no longer matches the (modified) file content, so feed_auth.py
        # must reject it.
        tampered = path.read_text() + "RH-99999|INJECTED|1900-01-01|???|???|99999.99|2024-01-01\n"
        path.write_text(tampered)
    return path


# ---------------------------------------------------------------------------
# Supplier 4: VitalRx Supply -- XML
# ---------------------------------------------------------------------------
def build_vitalrx_supply(n=25):
    items = []
    for i in range(n):
        drug, ndc = random.choice(DRUG_NAMES)
        items.append(
            f"  <shipment>\n"
            f"    <id>VRX-{7000+i}</id>\n"
            f"    <drug>{drug}</drug>\n"
            f"    <ndc>{ndc}</ndc>\n"
            f"    <lot>L{random.randint(100000,999999)}</lot>\n"
            f"    <qty>{random.randint(20, 3000)}</qty>\n"
            f"    <shipped>2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}</shipped>\n"
            f"  </shipment>"
        )
    content = "<?xml version=\"1.0\"?>\n<vitalrx_shipments>\n" + "\n".join(items) + \
        "\n</vitalrx_shipments>\n"
    return _write_signed("vitalrx_supply", "vitalrx_supply_20240601.xml", content)


# ---------------------------------------------------------------------------
# Supplier 5: SummitCare Claims -- fixed-width text
# ---------------------------------------------------------------------------
def build_summitcare_claims(n=30):
    # Fixed-width columns: claim_id(10) name(24) dob(10) dx(8) amount(10)
    lines = []
    for i in range(n):
        name, dob, _addr = _random_person()
        claim_id = f"SC-{30000+i}"
        amount = f"{random.uniform(70, 4500):.2f}"
        dx = random.choice(DIAGNOSIS_CODES)
        line = f"{claim_id:<10}{name:<24}{dob:<10}{dx:<8}{amount:<10}"
        lines.append(line)
    content = "\n".join(lines) + "\n"
    return _write_signed("summitcare_claims", "summitcare_claims_20240601.txt", content)


def main():
    print("Simulating supplier feeds ...")
    p1 = build_medplus_claims()
    p2 = build_pharmacorp_supply()
    p3a = build_regionalhealth_claims(corrupt=False)
    p3b = build_regionalhealth_claims(corrupt=True)
    p4 = build_vitalrx_supply()
    p5 = build_summitcare_claims()
    for p in [p1, p2, p3a, p3b, p4, p5]:
        print(f"  wrote {p.relative_to(ROOT)}  ({p.stat().st_size} bytes)")
    print("Done. 5 suppliers, 6 feed files (1 deliberately corrupted) written to data/incoming/.")


if __name__ == "__main__":
    main()
