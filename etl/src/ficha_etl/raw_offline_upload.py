"""Offline raw uploader composition: synthetic ZIP transport and live QRF sink.

Not wired to the production uploader or CLI. No discovery, real credentials,
network transport, overwrite, retry, or publication authority. The byte cap
bounds the staged payload retained by this call, not the whole Python RSS.
The sink may retain previous cohorts; use a fresh domain for a memory budget.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from . import raw_preflight
from .raw_authority import AuthorityRefused, Pin, _hex_id, _validate_month
from .raw_qrf import OfflineReconciler, Receipt, SyntheticSink
from .sources import canonical_inventory

_MAX_SYNTHETIC_BYTES = 64 * 1024**2


def run_offline_raw_upload(
    authority: OfflineReconciler,
    sink: SyntheticSink,
    month: str,
    intent: str,
    generation: str,
    transport: httpx.MockTransport,
    *,
    max_bytes: int = _MAX_SYNTHETIC_BYTES,
) -> tuple[Receipt, ...]:
    """Stage the canonical batch and reconcile each grant exactly once.

    Validate ALL immutable bodies before reserve or admit. Refuse a different
    live sink. A failed call is not a resumable upload: stop without retry.
    A lost return after terminal commit may recover only that exact receipt.
    """
    if (
        type(authority) is not OfflineReconciler
        or type(sink) is not SyntheticSink
        or authority._sink is not sink
        or type(transport) is not httpx.MockTransport
    ):
        raise AuthorityRefused("same synthetic domain and mock transport required")
    destination = f"ficha-{month}"
    _validate_month(month, destination)
    if not _hex_id(intent) or not _hex_id(generation):
        raise AuthorityRefused("invalid reservation")
    if type(max_bytes) is not int or not 0 < max_bytes <= _MAX_SYNTHETIC_BYTES:
        raise AuthorityRefused("invalid synthetic byte cap")
    specs = canonical_inventory()
    bodies: dict[str, bytes] = {}
    pins: list[Pin] = []
    with TemporaryDirectory() as temp:
        staged = raw_preflight.stage_all(
            specs, month, "synthetic-only", Path(temp), synthetic_transport=transport
        )
        if len(staged) != 37 or set(staged) != {spec.name for spec in specs}:
            raise AuthorityRefused("incomplete synthetic batch")
        total = sum(size for _, size, _ in staged.values())
        if total > max_bytes:
            raise AuthorityRefused("synthetic aggregate byte cap")
        for spec in specs:
            path, size, digest = staged[spec.name]
            if not 0 < size <= max_bytes:
                raise AuthorityRefused("invalid staged size")
            with path.open("rb") as handle:
                body = handle.read(size + 1)
            if len(body) != size or hashlib.sha256(body).hexdigest() != digest:
                raise AuthorityRefused("staged identity changed")
            bodies[spec.name] = body
            pins.append(Pin(spec.name, digest, size))
    authority.reserve(month, destination, intent, generation, pins)
    receipts: list[Receipt] = []
    for pin in pins:
        grant = authority.admit(month, pin.name)
        try:
            sink.apply_if_absent(grant, bodies[pin.name])
        except httpx.TransportError:
            # Response lost: no replay. Only fresh Q/R/CAS may resolve UNKNOWN.
            pass
        try:
            receipt = authority.reconcile(grant.claim)
        except OSError:
            # Distinguish terminal commit from lost return. Never reapply.
            recovered = authority.receipt_for_claim(grant.claim)
            if recovered is None:
                raise
            receipt = recovered
        if receipt != authority.receipt_for_claim(grant.claim):
            raise AuthorityRefused("receipt not bound to current reservation")
        receipts.append(receipt)
    if len(receipts) != 37 or len({receipt.token for receipt in receipts}) != 37:
        raise AuthorityRefused("incomplete current reservation receipts")
    return tuple(receipts)
