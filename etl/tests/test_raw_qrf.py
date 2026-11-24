"""QRF-01/02/03/04/05/06/09/10 controls on the actual offline composition."""

from __future__ import annotations

import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from ficha_etl.raw_authority import AuthorityRefused, LocalAuthority, Pin
from ficha_etl.raw_qrf import Grant, OfflineReconciler, SyntheticSink, historical_receipts
from ficha_etl.raw_readback import RawReadbackError
from ficha_etl.sources import canonical_inventory

MONTH = "2026-07"


def require(condition):
    if not condition:
        raise AssertionError("QRF control failed")


@pytest.fixture
def setup(tmp_path):
    path = tmp_path / "qrf.sqlite"
    sink = SyntheticSink()
    authority = OfflineReconciler(path, sink, create=True)
    bodies = {spec.name: spec.name.encode() for spec in canonical_inventory()}
    pins = tuple(Pin(n, hashlib.sha256(b).hexdigest(), len(b)) for n, b in bodies.items())
    authority.reserve(MONTH, f"ficha-{MONTH}", "a" * 32, "b" * 32, pins)
    return path, sink, authority, bodies


def test_qrf02_one_shot_two_threads(setup):
    path, sink, authority, bodies = setup
    name = next(iter(bodies))
    grant = authority.admit(MONTH, name)

    def apply():
        try:
            sink.apply(grant, bodies[name])
        except AuthorityRefused:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: apply(), range(2)))
    require(sorted(results) == [False, True])
    require(sink._sequence == 1)
    receipt = authority.reconcile(grant.claim)
    require(receipt.cut == 2 and receipt.observed == 3)
    require(historical_receipts(path) == (receipt,))
    with pytest.raises(AuthorityRefused, match="not active"):
        sink.apply(grant, bodies[name])


def test_qrf02_ambiguous_attempt_not_rearmed(setup):
    path, sink, authority, bodies = setup
    first, second = list(bodies)[:2]
    grant = authority.admit(MONTH, first)
    with pytest.raises(AuthorityRefused, match="ambiguous"):
        sink.apply(grant, bodies[first], fail_after_consume=True)
    with pytest.raises(AuthorityRefused, match="not active"):
        sink.apply(grant, bodies[first])
    with pytest.raises(RawReadbackError, match="complete HTTP 200"):
        authority.reconcile(grant.claim)
    with pytest.raises(AuthorityRefused, match="UNKNOWN"):
        authority.admit(MONTH, second)
    require(authority.lookup_unknown(MONTH) == grant.claim)
    require(historical_receipts(path) == ())


def test_qrf03_unpresented_grant_is_revoked_before_read(setup):
    path, sink, authority, bodies = setup
    name = next(iter(bodies))
    grant = authority.admit(MONTH, name)
    with pytest.raises(RawReadbackError, match="complete HTTP 200"):
        authority.reconcile(grant.claim)
    with pytest.raises(AuthorityRefused, match="not active"):
        sink.apply(grant, bodies[name])
    require(authority.lookup_unknown(MONTH) == grant.claim)
    require(historical_receipts(path) == ())


def test_qrf03_preexisting_bytes_reconcile_without_apply(setup):
    path, sink, authority, bodies = setup
    first, second = list(bodies)[:2]
    # Trusted fixture seeds bytes before this grant exists, not via apply.
    sink._objects[MONTH, first] = bodies[first]
    before = dict(sink._objects)
    grant = authority.admit(MONTH, first)
    require(sink._sequence == 0)
    receipt = authority.reconcile(grant.claim)
    require(receipt.cut == 1 and receipt.observed == 2)
    require(receipt.digest == grant.claim.pin.sha256)
    require(not receipt.publication_authorized)
    require(sink._objects == before)
    require(historical_receipts(path) == (receipt,))
    with pytest.raises(AuthorityRefused, match="not active"):
        sink.apply(grant, bodies[first])
    require(sink._sequence == 2 and sink._objects == before)
    require(authority.admit(MONTH, second).claim.pin.name == second)


def test_qrf01_identity_is_not_grant_and_wrong_claim_refused(setup):
    path, sink, authority, bodies = setup
    name = next(iter(bodies))
    grant = authority.admit(MONTH, name)
    claim = authority.lookup_unknown(MONTH)
    require(claim == grant.claim)
    with pytest.raises(AuthorityRefused, match="not active"):
        sink.apply(Grant(claim), bodies[name])
    with pytest.raises(AuthorityRefused, match="not current"):
        authority.reconcile(replace(claim, generation="f" * 32))
    require(sink._sequence == 0)
    require(historical_receipts(path) == ())
    sink.apply(grant, bodies[name])
    require(authority.reconcile(claim).digest == claim.pin.sha256)


def test_qrf05_wrong_bytes_keep_unknown(setup):
    path, sink, authority, bodies = setup
    name = next(iter(bodies))
    grant = authority.admit(MONTH, name)
    sink.apply(grant, b"x" * len(bodies[name]))
    with pytest.raises(RawReadbackError, match="differs"):
        authority.reconcile(grant.claim)
    require(authority.lookup_unknown(MONTH) == grant.claim)
    require(historical_receipts(path) == ())


def test_qrf06_same_domain_two_handles_not_new_sink(setup):
    path, sink, authority, bodies = setup
    name = next(iter(bodies))
    grant = authority.admit(MONTH, name)
    sink.apply(grant, bodies[name])
    other = SyntheticSink()
    # Identical bytes and labels do not establish the same protection domain.
    other._objects.update(sink._objects)
    other._epoch = sink._epoch
    with pytest.raises(AuthorityRefused, match="continuity"):
        OfflineReconciler(path, other)
    handle = OfflineReconciler(path, sink)
    receipt = handle.reconcile(handle.lookup_unknown(MONTH))
    require(receipt.observed > receipt.cut)
    require(not receipt.publication_authorized)
    require(historical_receipts(path) == (receipt,))
    with pytest.raises(AuthorityRefused, match="continuity"):
        OfflineReconciler(path, SyntheticSink())


def test_qrf09_atomic_receipt_failure_and_stale_claim(setup):
    path, sink, authority, bodies = setup
    first, second = list(bodies)[:2]
    grant = authority.admit(MONTH, first)
    sink.apply(grant, bodies[first])
    # Fault injection only: force the receipt INSERT to fail after UPDATE.
    with sqlite3.connect(path) as db:
        db.execute(
            """CREATE TRIGGER reject_receipt BEFORE INSERT ON qrf_receipt
               BEGIN SELECT RAISE(ABORT, 'receipt fault'); END"""
        )
    with pytest.raises(sqlite3.IntegrityError, match="receipt fault"):
        authority.reconcile(grant.claim)
    require(authority.lookup_unknown(MONTH) == grant.claim)
    require(historical_receipts(path) == ())
    with pytest.raises(AuthorityRefused, match="UNKNOWN"):
        authority.admit(MONTH, second)
    with sqlite3.connect(path) as db:
        db.execute("DROP TRIGGER reject_receipt")
    receipt = authority.reconcile(grant.claim)
    next_grant = authority.admit(MONTH, second)
    with pytest.raises(AuthorityRefused, match="not current"):
        authority.reconcile(grant.claim)
    require(authority.lookup_unknown(MONTH) == next_grant.claim)
    require(historical_receipts(path) == (receipt,))


def test_qrf09_terminal_commit_busy_after_real_readback(setup, monkeypatch):
    path, sink, authority, bodies = setup
    first, second = list(bodies)[:2]
    grant = authority.admit(MONTH, first)
    sink.apply(grant, bodies[first])
    connect = sqlite3.connect
    read = sink._read
    trace = []
    reader = connect(path, isolation_level=None, timeout=0)

    def traced_connect(*args, **kwargs):
        kwargs["timeout"] = 0
        db = connect(*args, **kwargs)
        db.set_trace_callback(trace.append)
        return db

    def read_then_lock(claim):
        observation = read(claim)
        # Acquire a SHARED lock only after the real read returns. An earlier
        # lock would fail the initial identity transaction, not terminal CAS.
        reader.execute("BEGIN")
        require(reader.execute("SELECT state FROM qrf_claim").fetchall() == [("UNKNOWN",)])
        return observation

    try:
        require(reader.execute("PRAGMA journal_mode").fetchone() == ("delete",))
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", traced_connect)
            patch.setattr(sink, "_read", read_then_lock)
            with pytest.raises(sqlite3.OperationalError) as error:
                authority.reconcile(grant.claim)
            require(error.value.sqlite_errorcode == sqlite3.SQLITE_BUSY)
            require(sink._sequence == 3)
            terminal = [sql.strip().upper() for sql in trace[-4:]]
            require(len(terminal) == 4)
            require(terminal[0].startswith("UPDATE QRF_CLAIM"))
            require(terminal[1].startswith("INSERT INTO QRF_RECEIPT"))
            require(terminal[2:] == ["COMMIT", "ROLLBACK"])
    finally:
        reader.close()

    require(authority.lookup_unknown(MONTH) == grant.claim)
    require(historical_receipts(path) == ())
    with pytest.raises(AuthorityRefused, match="not active"):
        sink.apply(grant, bodies[first])
    with pytest.raises(AuthorityRefused, match="UNKNOWN"):
        authority.admit(MONTH, second)
    require(sink._sequence == 3)
    receipt = authority.reconcile(grant.claim)
    require(receipt.cut == 4 and receipt.observed == 5)
    require(historical_receipts(path) == (receipt,))
    require(authority.admit(MONTH, second).claim.pin.name == second)


def test_qrf09_legacy_api_cannot_resolve(setup):
    path, sink, authority, bodies = setup
    grant = authority.admit(MONTH, next(iter(bodies)))
    with pytest.raises(sqlite3.OperationalError, match="no such table: intent"):
        LocalAuthority.reopen_for_test(path, current_unique_authority=True)
    require(authority.lookup_unknown(MONTH) == grant.claim)
    require(historical_receipts(path) == ())


def test_qrf10_all_37_sequential_without_promotion(setup):
    path, sink, authority, bodies = setup
    receipts = []
    for name, body in bodies.items():
        grant = authority.admit(MONTH, name)
        sink.apply(grant, body)
        receipt = authority.reconcile(grant.claim)
        require(not receipt.publication_authorized)
        receipts.append(receipt)
    require(len(receipts) == 37)
    require(historical_receipts(path) == tuple(receipts))
    require(authority.lookup_unknown(MONTH) is None)
    with pytest.raises(AuthorityRefused, match="already admitted"):
        authority.admit(MONTH, next(iter(bodies)))
