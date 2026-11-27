"""Characterize same-prefix staging collisions in the real shard ZIP writer (#495).

Synthetic rows only. No network, production data, upload, or manifest.
"""

from __future__ import annotations

import sys
import threading
import zipfile
from pathlib import Path

import pytest

from ficha_etl import sharded_pack
from ficha_etl.spooling_zip import SpoolingZipFile


class CrossAttemptStageRace(AssertionError):
    """A succeeded despite publishing a rival's materialization bytes."""


class _Company:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.snapshot_yyyymm = 0

    def SerializeToString(self) -> bytes:  # noqa: N802 -- protobuf API
        return self.payload


class _Materialization:
    def __init__(self, identity: str) -> None:
        self.identity = identity

    def meta_payload(self) -> dict[str, str]:
        return {"materialization_id": self.identity * 64}


def _setup(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[dict]]:
    monkeypatch.setattr(sharded_pack, "row_to_company", lambda row: _Company(row["payload"]))
    monkeypatch.setattr(sharded_pack, "company_root", lambda _company: "42000000")
    return {kind: [] for kind in sharded_pack.LOOKUP_KINDS}


def _pack(
    path: Path,
    payload: bytes,
    identity: str,
    lookups: dict[str, list[dict]],
) -> tuple[int, int]:
    return sharded_pack._pack_rows(  # noqa: SLF001 -- regression at staging boundary
        iter([{"payload": payload}]),
        lookups,
        path,
        snapshot_month="2026-07",
        materialization=_Materialization(identity),
        schema_desc=b"schema",
        schema_sha256="9" * 64,
    )


def _payload(path: Path) -> bytes:
    with zipfile.ZipFile(path) as archive:
        return archive.read(sharded_pack.cnpjpath("42000000"))


def test_separate_names_do_not_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Positive control: two different final names retain distinct contents."""
    lookups = _setup(monkeypatch)
    for name, content in (("companies-42.zip", b"A"), ("companies-43.zip", b"B")):
        path = tmp_path / name
        count, size = _pack(path, content, content.decode(), lookups)
        if count != 1 or size != path.stat().st_size or _payload(path) != content:
            raise AssertionError(f"bad isolated shard: {name}")


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX unlink of open file")
@pytest.mark.xfail(
    strict=True,
    raises=CrossAttemptStageRace,
    reason="fixed .part lets attempt A publish attempt B's ZIP",
)
def test_same_prefix_concurrent_attempts_are_isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A may never return success when the final ZIP contains B's bytes."""
    lookups = _setup(monkeypatch)
    a_open = threading.Event()
    b_open = threading.Event()
    a_done = threading.Event()
    real_writer = SpoolingZipFile

    class InterleavedZip(real_writer):
        def __enter__(self):
            value = super().__enter__()
            if threading.current_thread().name == "writer-A":
                a_open.set()
                if not b_open.wait(10):
                    raise AssertionError("B did not open its ZIP")
            elif threading.current_thread().name == "writer-B":
                b_open.set()
                if not a_done.wait(10):
                    raise AssertionError("A did not publish")
            return value

    monkeypatch.setattr(sharded_pack, "SpoolingZipFile", InterleavedZip)
    results: dict[str, tuple[int, int]] = {}
    errors: dict[str, BaseException] = {}
    path = tmp_path / "companies-42.zip"

    def worker(identity: str) -> None:
        try:
            results[identity] = _pack(path, identity.encode(), identity, lookups)
        except BaseException as exc:
            errors[identity] = exc
        finally:
            if identity == "A":
                a_done.set()

    a = threading.Thread(target=worker, args=("A",), name="writer-A")
    b = threading.Thread(target=worker, args=("B",), name="writer-B")
    a.start()
    if not a_open.wait(10):
        raise AssertionError("A did not reach stage open")
    b.start()
    a.join(20)
    b.join(20)
    if a.is_alive() or b.is_alive():
        raise AssertionError("staging interleave deadlocked")
    if errors.get("A") is not None:
        raise AssertionError(f"A failed unexpectedly: {errors['A']!r}")
    if not isinstance(errors.get("B"), FileNotFoundError):
        raise AssertionError(f"unexpected B outcome: {errors.get('B')!r}")
    if results.get("A", (0,))[0] != 1:
        raise AssertionError(f"A did not return successful count: {results!r}")
    actual = _payload(path)
    if actual == b"B":
        raise CrossAttemptStageRace("A completed, B failed, final holds B's protobuf")
    if actual != b"A":
        raise AssertionError(f"unexpected final bytes: {actual!r}")
