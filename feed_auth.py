"""
feed_auth.py

Real feed-authentication boundary: every inbound supplier file in
data/incoming/ must carry a matching `<filename>.sig` sidecar containing the
HMAC-SHA256 signature of the file's bytes, computed with that supplier's
shared secret key. This mirrors how a real claims/pharma clearinghouse
authenticates partner feeds before anything downstream (parsing, loading)
ever touches the content.

A feed is REJECTED (moved to data/rejected/, never parsed) if:
  - its .sig sidecar is missing, or
  - the recomputed HMAC does not match the signature on file (tamper /
    corruption detection), or
  - its filename does not map to a known, trusted supplier key.

This is the control that makes the deliberately-corrupted RegionalHealth
batch-2 feed fail closed instead of silently entering the pipeline.
"""
import hashlib
import hmac
import shutil
from dataclasses import dataclass
from pathlib import Path

from simulate_supplier_feeds import SUPPLIER_KEYS

ROOT = Path(__file__).parent
INCOMING = ROOT / "data" / "incoming"
REJECTED = ROOT / "data" / "rejected"
REJECTED.mkdir(parents=True, exist_ok=True)

# Map filename prefixes -> supplier key id, so we know which secret to check
# a given file against (mirrors a real registry of trusted trading partners).
SUPPLIER_PREFIXES = {
    "medplus_claims": "medplus_claims",
    "pharmacorp_supply": "pharmacorp_supply",
    "regionalhealth_claims": "regionalhealth_claims",
    "vitalrx_supply": "vitalrx_supply",
    "summitcare_claims": "summitcare_claims",
}


@dataclass
class AuthResult:
    path: Path
    supplier: str
    authenticated: bool
    reason: str


def _supplier_for(filename: str):
    for prefix, supplier in SUPPLIER_PREFIXES.items():
        if filename.startswith(prefix):
            return supplier
    return None


def authenticate_feed(path: Path) -> AuthResult:
    supplier = _supplier_for(path.name)
    if supplier is None:
        return AuthResult(path, "unknown", False, "unrecognized supplier / no trusted key")

    sig_path = Path(str(path) + ".sig")
    if not sig_path.exists():
        return AuthResult(path, supplier, False, "missing .sig signature file")

    expected_sig = sig_path.read_text().strip()
    data = path.read_bytes()
    key = SUPPLIER_KEYS[supplier]
    actual_sig = hmac.new(key, data, hashlib.sha256).hexdigest()

    if hmac.compare_digest(expected_sig, actual_sig):
        return AuthResult(path, supplier, True, "signature verified")
    return AuthResult(path, supplier, False,
                       f"signature mismatch (expected {expected_sig[:12]}..., "
                       f"got {actual_sig[:12]}...) -- file content does not match signed content")


def authenticate_all(move_rejected: bool = True):
    """Authenticate every feed file in data/incoming/. Returns list[AuthResult].

    Authenticated feeds are left in place for the ETL step to load.
    Failed feeds are moved to data/rejected/ (quarantined, never loaded) when
    move_rejected=True.
    """
    results = []
    feed_files = sorted(
        p for p in INCOMING.glob("*")
        if not p.name.endswith(".sig")
    )
    for path in feed_files:
        result = authenticate_feed(path)
        results.append(result)
        if not result.authenticated and move_rejected:
            dest = REJECTED / path.name
            shutil.copy2(path, dest)
            sig_src = Path(str(path) + ".sig")
            if sig_src.exists():
                shutil.copy2(sig_src, REJECTED / sig_src.name)
    return results


def main():
    results = authenticate_all()
    print(f"Authenticated {sum(r.authenticated for r in results)}/{len(results)} feed files:\n")
    for r in results:
        status = "PASS" if r.authenticated else "FAIL"
        print(f"  [{status}] {r.path.name:45s} supplier={r.supplier:24s} {r.reason}")
    return results


if __name__ == "__main__":
    main()
