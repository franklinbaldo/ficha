"""Characterize a false local receipt after same-prefix staging collision (#495).

Only synthetic rows and a fake DuckDB cursor; actual shard pack and receipt
functions are exercised. No remote operations, uploads or production data.
"""

from __future__ import annotations

import hashlib
import json
import sys
import threading
import zipfile
from pathlib import Path

import pytest

from ficha_etl import sharded_pack
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY
from ficha_etl.shard_transfer import prepare_one_shard
from ficha_etl.spooling_zip import SpoolingZipFile


class CrossAttemptReceiptRace(AssertionError):
    """Receipt for materialization A binds the complete ZIP produced by B."""


class _SyntheticCompany:
    snapshot_yyyymm = 0

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def SerializeToString(self) -> bytes:  # noqa: N802 -- protobuf API
        return self.payload


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX unlink of open file")
@pytest.mark.xfail(
    strict=True,
    raises=CrossAttemptReceiptRace,
    reason="local receipt labels B's ZIP with A's semantic materialization id",
)
def test_prepared_receipt_does_not_bind_rival_zip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A's successful receipt must identify the materialization embedded in its ZIP."""
    open_a = threading.Event()
    open_b = threading.Event()
    replaced_a = threading.Event()
    finished_b = threading.Event()
    path = tmp_path / "companies-42.zip"
    lookups = {kind: [] for kind in sharded_pack.LOOKUP_KINDS}
    urls = {kind: "unused" for kind in ("raizes", "cnpjs", "socios")}
    pins = {"A": {"cnpjs.parquet": "a" * 40}, "B": {"cnpjs.parquet": "b" * 40}}
    sessions = {
        identity: sharded_pack.ShardPackSession(
            "2026-07", PUBLIC_COMPANIES_GEOMETRY, parquets_base="unused"
        )
        for identity in pins
    }
    for session in sessions.values():
        monkeypatch.setattr(session, "_require_open", lambda: (object(), lookups, urls))

    def fake_rows(*_args, **_kwargs):
        return iter([{"payload": threading.current_thread().name.encode("ascii")}])

    monkeypatch.setattr(sharded_pack, "_iter_rows_for_range", fake_rows)
    monkeypatch.setattr(
        sharded_pack, "row_to_company", lambda row: _SyntheticCompany(row["payload"])
    )
    monkeypatch.setattr(sharded_pack, "company_root", lambda _company: "42000000")

    class InterleavedZip(SpoolingZipFile):
        def __enter__(self):
            archive = super().__enter__()
            if threading.current_thread().name == "writer-A":
                open_a.set()
                if not open_b.wait(10):
                    raise AssertionError("B did not open the colliding pathname")
            elif threading.current_thread().name == "writer-B":
                open_b.set()
                if not replaced_a.wait(10):
                    raise AssertionError("A did not replace B's partial pathname")
            return archive

    monkeypatch.setattr(sharded_pack, "SpoolingZipFile", InterleavedZip)
    original_replace = Path.replace

    def synchronized_replace(self: Path, target: Path) -> Path:
        result = original_replace(self, target)
        if threading.current_thread().name == "writer-A" and self.name.endswith(".part"):
            replaced_a.set()
            if not finished_b.wait(10):
                raise AssertionError("B did not finish the moved ZIP before A's size check")
        return result

    monkeypatch.setattr(Path, "replace", synchronized_replace)
    receipts: dict[str, object] = {}
    errors: dict[str, BaseException] = {}

    def worker(identity: str) -> None:
        try:
            receipts[identity] = prepare_one_shard(
                sessions[identity], "42", tmp_path, pinned_inputs=pins[identity]
            )
        except BaseException as exc:
            errors[identity] = exc
        finally:
            if identity == "B":
                finished_b.set()

    a = threading.Thread(target=worker, args=("A",), name="writer-A")
    b = threading.Thread(target=worker, args=("B",), name="writer-B")
    a.start()
    if not open_a.wait(10):
        raise AssertionError("A did not open its ZIP")
    b.start()
    a.join(20)
    b.join(20)
    if a.is_alive() or b.is_alive():
        raise AssertionError("receipt interleaving deadlocked")
    if errors.get("A") is not None:
        raise AssertionError(f"A did not prepare a receipt: {errors['A']!r}")
    if not isinstance(errors.get("B"), FileNotFoundError):
        raise AssertionError(f"unexpected B outcome: {errors.get('B')!r}")
    receipt_a = receipts.get("A")
    if receipt_a is None:
        raise AssertionError("A returned without a receipt")

    expected_a = sessions["A"].materialization_spec("42", input_sha1s=pins["A"])
    expected_b = sessions["B"].materialization_spec("42", input_sha1s=pins["B"])
    if receipt_a.materialization_id != expected_a.materialization_id():
        raise AssertionError("receipt did not use A's semantic identity")
    if receipt_a.size != path.stat().st_size:
        raise AssertionError("A's size receipt does not match the final file")
    sha1 = hashlib.sha1(path.read_bytes(), usedforsecurity=False).hexdigest()
    if receipt_a.sha1 != sha1:
        raise AssertionError("A's SHA1 receipt does not match the final file")
    with zipfile.ZipFile(path) as archive:
        if archive.testzip() is not None:
            raise AssertionError("final ZIP has an invalid CRC")
        body = archive.read(sharded_pack.cnpjpath("42000000"))
        embedded = json.loads(archive.read("_meta.json"))["materialization"]["id"]

    if embedded == expected_a.materialization_id() and body == b"writer-A":
        return  # A fixed implementation makes this an XPASS(strict).
    if embedded == expected_b.materialization_id() and body == b"writer-B":
        raise CrossAttemptReceiptRace("A's receipt hashes B's valid ZIP but labels it as A")
    raise AssertionError(f"unexpected final ZIP: id={embedded}, payload={body!r}")


@pytest.mark.xfail(
    strict=True,
    raises=CrossAttemptReceiptRace,
    reason="a later complete pack can replace the final pathname before A seals its receipt",
)
def test_portable_final_path_replacement_can_relabel_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cross-platform race: no unlink of an open file, no overlapping ZIP writers.

    A has completely closed and renamed its ZIP before B begins.  A is paused
    immediately after rename, before _pack_rows reads size / prepare hashes.
    B then completes its own pack to the SAME final path.  Both calls return
    success; A may sign B's bytes with A's materialization_id.
    """
    renamed_a = threading.Event()
    completed_b = threading.Event()
    pins = {
        "A": {"cnpjs.parquet": "a" * 40},
        "B": {"cnpjs.parquet": "b" * 40},
    }
    lookups = {kind: [] for kind in sharded_pack.LOOKUP_KINDS}
    urls = {kind: "unused" for kind in ("raizes", "cnpjs", "socios")}
    sessions = {
        name: sharded_pack.ShardPackSession(
            "2026-07", PUBLIC_COMPANIES_GEOMETRY, parquets_base="unused"
        )
        for name in pins
    }
    for session in sessions.values():
        monkeypatch.setattr(session, "_require_open", lambda: (object(), lookups, urls))
    monkeypatch.setattr(
        sharded_pack,
        "_iter_rows_for_range",
        lambda *_args, **_kwargs: iter(
            [{"payload": threading.current_thread().name.encode("ascii")}]
        ),
    )
    monkeypatch.setattr(
        sharded_pack, "row_to_company", lambda row: _SyntheticCompany(row["payload"])
    )
    monkeypatch.setattr(sharded_pack, "company_root", lambda _company: "42000000")

    original_replace = Path.replace
    canonical = tmp_path / "companies-42.zip"

    def pause_after_a_rename(source: Path, target: Path) -> Path:
        result = original_replace(source, target)
        if threading.current_thread().name == "attempt-A" and target == canonical:
            renamed_a.set()
            if not completed_b.wait(20):
                raise AssertionError("B did not complete after A's finalized rename")
        return result

    monkeypatch.setattr(Path, "replace", pause_after_a_rename)
    receipts: dict[str, object] = {}
    errors: dict[str, BaseException] = {}

    def worker(identity: str) -> None:
        try:
            receipts[identity] = prepare_one_shard(
                sessions[identity], "42", tmp_path, pinned_inputs=pins[identity]
            )
        except BaseException as exc:
            errors[identity] = exc
        finally:
            if identity == "B":
                completed_b.set()

    first = threading.Thread(target=worker, args=("A",), name="attempt-A")
    second = threading.Thread(target=worker, args=("B",), name="attempt-B")
    first.start()
    if not renamed_a.wait(20):
        completed_b.set()
        first.join(20)
        raise AssertionError(f"A did not finalize its ZIP: {errors.get('A')!r}")
    second.start()
    first.join(30)
    second.join(30)
    if first.is_alive() or second.is_alive():
        raise AssertionError("portable final-path interleaving deadlocked")
    if errors:
        raise AssertionError(f"both packs should succeed: {errors!r}")
    receipt_a, receipt_b = receipts["A"], receipts["B"]
    expected_a = sessions["A"].materialization_spec("42", input_sha1s=pins["A"])
    expected_b = sessions["B"].materialization_spec("42", input_sha1s=pins["B"])
    if receipt_a.materialization_id != expected_a.materialization_id():
        raise AssertionError("A receipt lost its expected identity")
    if receipt_b.materialization_id != expected_b.materialization_id():
        raise AssertionError("B receipt lost its expected identity")

    def actual(path: Path) -> tuple[str, bytes, int, str]:
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                raise AssertionError("a completed ZIP must pass CRC")
            embedded = json.loads(archive.read("_meta.json"))["materialization"]["id"]
            body = archive.read(sharded_pack.cnpjpath("42000000"))
        data = path.read_bytes()
        return embedded, body, len(data), hashlib.sha1(
            data, usedforsecurity=False
        ).hexdigest()

    embedded_a, body_a, size_a, sha_a = actual(receipt_a.path)
    embedded_b, body_b, size_b, sha_b = actual(receipt_b.path)
    if (embedded_b, body_b) != (expected_b.materialization_id(), b"attempt-B"):
        raise AssertionError("B's completed pack must retain its own materialization")
    if (receipt_b.size, receipt_b.sha1) != (size_b, sha_b):
        raise AssertionError("B's receipt is not physically consistent")
    if (receipt_a.size, receipt_a.sha1) != (size_a, sha_a):
        raise AssertionError("A's receipt does not match the bytes now at its path")

    # The real opt-in planner validates local size+sha1 again. It does not
    # inspect the embedded _meta.json and therefore accepts the bad identity.
    from ficha_etl.shard_transfer import plan_opt_in_v11_shard

    planned_a = plan_opt_in_v11_shard(
        sessions["A"], receipt_a, pinned_inputs=pins["A"]
    )
    if planned_a.materialization_id != expected_a.materialization_id():
        raise AssertionError("planner changed A's declared materialization ID")
    if (planned_a.size, planned_a.sha1) != (size_a, sha_a):
        raise AssertionError("planner did not attest the currently read bytes")

    if (embedded_a, body_a) == (expected_a.materialization_id(), b"attempt-A"):
        return  # Unique immutable final artifact makes this XPASS(strict).
    if (embedded_a, body_a) == (expected_b.materialization_id(), b"attempt-B"):
        raise CrossAttemptReceiptRace(
            "A's opt-in plan labels B's valid completed ZIP as materialization A"
        )
    raise AssertionError(f"unanticipated A ZIP identity: {embedded_a}, {body_a!r}")
