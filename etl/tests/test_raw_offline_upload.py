"""Exercise the real stage_all and offline QRF uploader together."""

from __future__ import annotations

import hashlib
import io
import sqlite3
import zipfile
from dataclasses import replace

import httpx
import pytest

from ficha_etl import raw_preflight, upstream
from ficha_etl.raw_authority import AuthorityRefused, Pin
from ficha_etl.raw_offline_upload import run_offline_raw_upload
from ficha_etl.raw_qrf import OfflineReconciler, SyntheticSink, historical_receipts
from ficha_etl.raw_readback import RawReadbackError
from ficha_etl.sources import canonical_inventory

MONTH = "2026-07"


def require(condition):
    if not condition:
        raise AssertionError("offline uploader control failed")


@pytest.fixture
def setup(tmp_path, monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("real network forbidden")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", no_network)
    monkeypatch.setattr(upstream, "discover_token", no_network)
    path = tmp_path / "qrf.sqlite"
    sink = SyntheticSink()
    authority = OfflineReconciler(path, sink, create=True)
    bodies = {}
    for i, spec in enumerate(canonical_inventory()):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            archive.writestr(zipfile.ZipInfo("fixture.txt"), bytes([i]))
        bodies[spec.name] = output.getvalue()
    return path, sink, authority, bodies


def transport_for(bodies, *, redirect=None, requests=None):
    def handler(request):
        if requests is not None:
            requests.append(str(request.url))
        require(request.method == "GET")
        name = request.url.path.rsplit("/", 1)[-1]
        if name == redirect:
            return httpx.Response(302, headers={"Location": "https://example.invalid/target"})
        body = bodies[name]
        return httpx.Response(
            200, headers={"Content-Length": str(len(body))}, stream=httpx.ByteStream(body)
        )

    return httpx.MockTransport(handler)


def run(setup, *, transport=None, max_bytes=65536):
    _, sink, authority, bodies = setup
    return run_offline_raw_upload(
        authority,
        sink,
        MONTH,
        "a" * 32,
        "b" * 32,
        transport if transport is not None else transport_for(bodies),
        max_bytes=max_bytes,
    )


def counts(path, month=None):
    with sqlite3.connect(path) as db:
        if month is None:
            return tuple(
                db.execute(f"SELECT count(*) FROM qrf_{table}").fetchone()[0]
                for table in ("intent", "pin", "claim", "receipt")
            )
        return db.execute(
            """SELECT count(*) FROM qrf_receipt r JOIN qrf_claim c ON c.token=r.token
               WHERE c.month=? AND c.state='TEST_MATCHED'""",
            (month,),
        ).fetchone()[0]


def test_exact_aggregate_cap_and_preexisting_match(setup, monkeypatch):
    path, sink, _, bodies = setup
    first = next(iter(bodies))
    sink._objects[MONTH, first] = bodies[first]
    apply = sink.apply_if_absent
    effects = []

    def tracked(grant, body):
        result = apply(grant, body)
        effects.append(result)
        return result

    monkeypatch.setattr(sink, "apply_if_absent", tracked)
    receipts = run(setup, max_bytes=sum(map(len, bodies.values())))
    require(len(receipts) == 37)
    require(effects == [False] + [True] * 36)
    require(counts(path) == (1, 37, 37, 37))
    require(all(not receipt.publication_authorized for receipt in receipts))
    require(sink._objects == {(MONTH, name): body for name, body in bodies.items()})


def test_cap_minus_one_before_reserve(setup):
    path, sink, _, bodies = setup
    with pytest.raises(AuthorityRefused, match="aggregate byte cap"):
        run(setup, max_bytes=sum(map(len, bodies.values())) - 1)
    require(counts(path) == (0, 0, 0, 0))
    require(sink._sequence == 0 and not sink._objects and not sink._grants)


def test_last_redirect_before_reserve(setup):
    path, sink, _, bodies = setup
    requests = []
    with pytest.raises(RuntimeError, match="redirected RFB source"):
        run(
            setup,
            transport=transport_for(bodies, redirect=list(bodies)[-1], requests=requests),
        )
    require(len(requests) == 37 and not any("example.invalid" in url for url in requests))
    require(counts(path) == (0, 0, 0, 0))
    require(not sink._grants and not sink._objects)


def test_last_two_valid_zip_paths_swapped_before_reserve(setup, monkeypatch):
    path, sink, _, bodies = setup
    stage = raw_preflight.stage_all

    def swapped(*args, **kwargs):
        staged = stage(*args, **kwargs)
        a, b = list(bodies)[-2:]
        pa, sa, ha = staged[a]
        pb, sb, hb = staged[b]
        require(sa == sb and ha != hb)
        staged[a] = (pb, sa, ha)
        staged[b] = (pa, sb, hb)
        return staged

    monkeypatch.setattr(raw_preflight, "stage_all", swapped)
    with pytest.raises(AuthorityRefused, match="staged identity changed"):
        run(setup)
    require(counts(path) == (0, 0, 0, 0))
    require(not sink._grants and not sink._objects)


def test_ack_lost_after_apply_and_return_lost_after_commit(setup, monkeypatch):
    path, sink, authority, bodies = setup
    apply = sink.apply_if_absent
    reconcile = authority.reconcile
    calls = []
    claims = []

    def lose_ack(grant, body):
        calls.append(grant.claim.token)
        result = apply(grant, body)
        if len(calls) == 1:
            raise httpx.ReadError("synthetic response lost")
        return result

    def lose_return(claim):
        claims.append(claim)
        result = reconcile(claim)
        if len(claims) == 1:
            raise OSError("synthetic return lost after commit")
        return result

    monkeypatch.setattr(sink, "apply_if_absent", lose_ack)
    monkeypatch.setattr(authority, "reconcile", lose_return)
    receipts = run(setup)
    require(len(calls) == len(set(calls)) == len(claims) == 37)
    require(authority.receipt_for_claim(claims[0]) == receipts[0])
    require(authority.receipt_for_claim(replace(claims[0], generation="c" * 32)) is None)
    require(counts(path) == (1, 37, 37, 37))
    require(sink._objects == {(MONTH, name): body for name, body in bodies.items()})


def test_terminal_busy_after_readback_stops_with_unknown(setup, monkeypatch):
    path, sink, authority, bodies = setup
    connect = sqlite3.connect
    reader = connect(path, isolation_level=None, timeout=0)
    read = sink._read
    trace = []

    def traced(*args, **kwargs):
        kwargs["timeout"] = 0
        db = connect(*args, **kwargs)
        db.set_trace_callback(trace.append)
        return db

    def read_then_lock(claim):
        observation = read(claim)
        reader.execute("BEGIN")
        require(reader.execute("SELECT state FROM qrf_claim").fetchall() == [("UNKNOWN",)])
        return observation

    try:
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", traced)
            patch.setattr(sink, "_read", read_then_lock)
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                run(setup)
        require(any("INSERT INTO qrf_receipt" in sql for sql in trace))
        require(trace[-2:] == ["COMMIT", "ROLLBACK"])
    finally:
        reader.close()
    claim = authority.lookup_unknown(MONTH)
    require(claim is not None and authority.receipt_for_claim(claim) is None)
    require(counts(path) == (1, 37, 1, 0))
    require(len(sink._objects) == 1)


def test_36_current_plus_one_other_month_not_success(setup):
    path, sink, authority, bodies = setup
    other = "2026-06"
    pins = [Pin(n, hashlib.sha256(b).hexdigest(), len(b)) for n, b in bodies.items()]
    authority.reserve(other, f"ficha-{other}", "c" * 32, "d" * 32, pins)
    last = list(bodies)[-1]
    old = authority.admit(other, last)
    sink.apply_if_absent(old, bodies[last])
    historical = authority.reconcile(old.claim)

    # Incomplete catalog is irrelevant. The live sink has divergent bytes.
    divergent = b"x" * len(bodies[last])
    sink._objects[MONTH, last] = divergent
    with pytest.raises(RawReadbackError, match="differs"):
        run(setup)
    require(sink._objects[MONTH, last] == divergent)
    require(counts(path, MONTH) == 36 and counts(path, other) == 1)
    require(len(historical_receipts(path)) == 37)
    claim = authority.lookup_unknown(MONTH)
    require(claim.pin.name == last and authority.receipt_for_claim(claim) is None)
    require(authority.receipt_for_claim(old.claim) == historical)
    require(authority.receipt_for_claim(replace(old.claim, month=MONTH)) is None)
