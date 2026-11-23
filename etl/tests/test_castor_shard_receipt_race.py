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
