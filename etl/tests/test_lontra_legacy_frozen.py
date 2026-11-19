"""Offline regression: legacy CLI uploads only descriptor-bound anonymous handles."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from ficha_etl import cli, legacy_upload_snapshot


class _Progress:
    def start(self):
        pass

    def add_task(self, *a, **kw):
        return 0

    def update(self, *a, **kw):
        pass

    def stop(self):
        pass


def _data() -> dict[str, bytes]:
    names = list(legacy_upload_snapshot._BASE_FILES.values())
    names += [f"lookups/{kind}.parquet" for kind in cli.transform._LOOKUP_KINDS]
    return {name: (name + " ORIGINAL ").encode() for name in names}


def _entry(data: dict[str, bytes]) -> dict:
    def descriptor(b):
        return {
            "size": len(b),
            "sha1": hashlib.sha1(b, usedforsecurity=False).hexdigest(),
            "sha256": hashlib.sha256(b).hexdigest(),
        }

    return {
        "files": {
            key: descriptor(data[name]) for key, name in legacy_upload_snapshot._BASE_FILES.items()
        },
        "lookups": {
            kind: descriptor(data[f"lookups/{kind}.parquet"])
            for kind in cli.transform._LOOKUP_KINDS
        },
    }


def _install(monkeypatch, tmp_path: Path, *, mutate=None, before_freeze=None, ia_replies=None):
    data = _data()
    output = tmp_path / "output"
    events = []
    sent = {}

    def transform(*args, **kwargs):
        assert kwargs["reject_empty_estabelecimentos"]
        for name, b in data.items():
            if name == "companies.zip":
                continue
            path = output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b)
        events.append("transform")

    def pack(*args, **kwargs):
        (output / "companies.zip").write_bytes(data["companies.zip"])
        events.append("pack")
        return {"count": 1, "size_bytes": len(data["companies.zip"])}

    def manifest(*args, **kwargs):
        entry = _entry(data)
        events.append("manifest-build")
        if before_freeze:
            before_freeze(output, data)
        return entry

    def raw(*args, **kwargs):
        events.append("raw")
        if mutate:
            mutate(output, data)

    def upload(identifier, *, files, **kwargs):
        # This fake consumes the *actual* handles passed by the uploader.
        # No requests leave this process.
        for name, handle in files.items():
            assert hasattr(handle, "read"), f"mutable pathname was passed for {name}"
            sent[name] = handle.read()
        events.append("ia-upload")
        if ia_replies is not None:
            return ia_replies(len(files))
        return [SimpleNamespace(status_code=200, url="offline") for _ in files]

    monkeypatch.setattr(cli, "make_progress", _Progress)
    monkeypatch.setattr(cli.transform, "transform_snapshot", transform)
    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", pack)
    monkeypatch.setattr(cli.manifest_mod, "build_snapshot_entry", manifest)
    monkeypatch.setattr(cli.upload_mod, "stream_raw_zips_to_ia", raw)
    monkeypatch.setattr(cli.upload_mod.ia, "upload", upload)
    monkeypatch.setattr(cli.manifest_mod, "verify_snapshot_files", lambda *a: [])
    monkeypatch.setattr(
        cli.manifest_mod, "update_manifest", lambda *a: events.append("manifest-update")
    )
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic")

    def run():
        return cli._cmd_run(
            "2026-08",
            cache_dir=tmp_path / "cache",
            output_dir=output,
            manifest_path=tmp_path / "manifest.json",
            skip_upload=False,
            verify=False,
            verify_sample_size=1,
        )

    return run, output, data, sent, events


@pytest.mark.parametrize("mutation", ["none", "rewrite", "replace"])
def test_legacy_cli_stable_bytes_after_postfreeze_source_mutation(monkeypatch, tmp_path, mutation):
    def change(output, data):
        if mutation == "rewrite":
            (output / "cnpjs.parquet").write_bytes(b"X" * len(data["cnpjs.parquet"]))
            (output / "companies.zip").write_bytes(b"Y" * len(data["companies.zip"]))
        elif mutation == "replace":
            for name in ("cnpjs.parquet", "companies.zip"):
                p = output / name
                other = p.with_suffix(".new")
                other.write_bytes(b"Z" * len(data[name]))
                os.replace(other, p)

    run, output, original, sent, events = _install(monkeypatch, tmp_path, mutate=change)
    assert run() == 0
    assert sent == original
    assert events == [
        "transform",
        "pack",
        "manifest-build",
        "raw",
        "ia-upload",
        "ia-upload",
        "manifest-update",
    ]


@pytest.mark.parametrize("mutation", ["rewrite", "replace"])
def test_legacy_cli_bad_snapshot_fails_before_first_raw_put(monkeypatch, tmp_path, mutation):
    def change(output, data):
        path = output / "cnpjs.parquet"
        if mutation == "rewrite":
            path.write_bytes(b"X" * len(data["cnpjs.parquet"]))
        else:
            other = output / "different"
            other.write_bytes(b"X" * len(data["cnpjs.parquet"]))
            os.replace(other, path)

    run, _, _, sent, events = _install(monkeypatch, tmp_path, before_freeze=change)
    assert run() == 1
    assert events == ["transform", "pack", "manifest-build"]
    assert sent == {}


def test_legacy_cli_scratch_failure_fails_before_raw(monkeypatch, tmp_path):
    run, _, _, sent, events = _install(monkeypatch, tmp_path)

    def no_space(*args, **kwargs):
        raise OSError("ENOSPC synthetic")

    monkeypatch.setattr(legacy_upload_snapshot.tempfile, "TemporaryFile", no_space)
    assert run() == 1
    assert events == ["transform", "pack", "manifest-build"]
    assert not sent


def test_legacy_cli_no_manifest_promotion_on_empty_ia_receipts(monkeypatch, tmp_path):
    run, _, _, _, events = _install(monkeypatch, tmp_path, ia_replies=lambda size: [])
    assert run() == 1
    assert "manifest-update" not in events


def test_legacy_cli_no_manifest_promotion_on_partial_ia_receipts(monkeypatch, tmp_path):
    def partial(size):
        assert size > 1
        return [SimpleNamespace(status_code=200, url="offline")]

    run, _, _, _, events = _install(monkeypatch, tmp_path, ia_replies=partial)
    assert run() == 1
    assert "manifest-update" not in events


def test_freeze_rejects_manifest_file_set_or_digest_mismatch(tmp_path):
    from contextlib import ExitStack

    data = _data()
    for name, b in data.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b)
    entry = _entry(data)
    with ExitStack() as stack:
        assert set(legacy_upload_snapshot.freeze_uploads(stack, tmp_path, entry)) == set(data)
    entry["files"].pop("cnpjs")
    with ExitStack() as stack, pytest.raises(ValueError, match="missing or extra"):
        legacy_upload_snapshot.freeze_uploads(stack, tmp_path, entry)
    entry = _entry(data)
    entry["lookups"][next(iter(entry["lookups"]))]["sha256"] = "0" * 64
    with ExitStack() as stack, pytest.raises(ValueError, match="copied bytes"):
        legacy_upload_snapshot.freeze_uploads(stack, tmp_path, entry)
