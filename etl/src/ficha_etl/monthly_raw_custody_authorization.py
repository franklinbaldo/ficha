"""Independent raw ZIP custody gate for monthly publication (#433).

Parsed-source v1 receipts remain readable as historical data. They cannot
authorize derived uploads or finalization, and descriptor claims are not
self-authenticating. There is deliberately no production authority adapter yet.
"""

from __future__ import annotations

from typing import Protocol

RAW_CUSTODY_SCHEMA = "ficha-raw-custody-receipt-v2"
RAW_CONFIRMED = "REMOTE_VERSION_CONFIRMED"
_REQUIRED_REFERENCES = (
    "cohort_witness_ref",
    "remote_bytes_witness_ref",
    "fence_witness_ref",
    "authority_claim_ref",
)


class RawCustodyAuthority(Protocol):
    """Independent monotonic authority, never the descriptor itself."""

    def confirm(self, *, month: str, run_id: str, claims: dict) -> bool: ...


def _require_raw_custody_authorization(
    month: str,
    descriptor: dict,
    *,
    authority: RawCustodyAuthority | None = None,
) -> None:
    """Fail closed unless independent S/R/F authority confirms exact attempt.

    A fake authority may be injected in isolated unit tests to exercise API
    shape; it is not evidence of remote custody or publication finality.
    """
    parsed = descriptor.get("source_attestation")
    raw = descriptor.get("raw_custody")
    if not isinstance(parsed, dict) or not isinstance(raw, dict):
        raise ValueError("raw custody v2 required; legacy v1 descriptor is historical/read-only")
    if (
        raw.get("schema") != RAW_CUSTODY_SCHEMA
        or raw.get("status") != RAW_CONFIRMED
        or raw.get("month") != month
        or raw.get("run_id") != parsed.get("run_id")
        or type(raw.get("inventory_count")) is not int
        or raw["inventory_count"] != 37
    ):
        raise ValueError("raw custody v2 lacks confirmed 37/37 source/remote/fence state")
    pin = raw.get("pin_set_sha256")
    if not (
        isinstance(pin, str)
        and len(pin) == 64
        and pin == pin.lower()
        and all(c in "0123456789abcdef" for c in pin)
    ):
        raise ValueError("raw custody v2 lacks canonical source pin-set digest")
    if any(
        not isinstance(raw.get(key), str)
        or not raw[key].strip()
        or len(raw[key]) > 512
        for key in _REQUIRED_REFERENCES
    ):
        raise ValueError("raw custody v2 lacks external S/R/F/authority references")
    if authority is None:
        raise ValueError("raw custody v2 HOLD: independent monotonic authority unavailable")
    if authority.confirm(month=month, run_id=raw["run_id"], claims=raw) is not True:
        raise ValueError("raw custody v2 HOLD: independent S/R/F authority did not confirm")
