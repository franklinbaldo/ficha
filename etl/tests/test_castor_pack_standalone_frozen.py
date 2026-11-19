"""Offline regression for standalone `ficha-etl pack` ZIP snapshot (#361)."""

from __future__ import annotations

import os
import tempfile
from contextlib import ExitStack

import pytest

from ficha_etl import cli, pack_upload_snapshot


@pytest.mark.parametrize("mutation", ["rewrite", "replace"])
def test_pack_cli_passes_anonymous_bytes_even_if_path_changes(monkeypatch, tmp_path, mutation):
    output = tmp_path / "companies.zip"
    original = b"PACKED ORIGINAL"
    events = []

    def pack(*args, **kwargs):
        output.write_bytes(original)
        events.append("pack")
        return {"count": 1, "size_bytes": len(original)}

    def upload(month, path, *, access_key, secret_key, frozen):
        assert path == output
        assert hasattr(frozen, "read") and not isinstance(frozen, str)
        if mutation == "rewrite":
            output.write_bytes(b"Z" * len(original))
        else:
            replacement = tmp_path / "replacement.zip"
            replacement.write_bytes(b"X" * len(original))
            os.replace(replacement, output)
        assert frozen.read() == original
        events.append("upload")

    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", pack)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", upload)
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic")
    assert (
        cli._cmd_pack(
            "2026-08",
            output=output,
            local_parquets=None,
            memory_limit_gb=None,
            skip_upload=False,
        )
        == 0
    )
    assert events == ["pack", "upload"]


def test_pack_cli_skip_upload_does_not_require_snapshot_or_credentials(monkeypatch, tmp_path):
    output = tmp_path / "companies.zip"

    def pack(*args, **kwargs):
        output.write_bytes(b"PACK")
        return {"count": 1, "size_bytes": 4}

    def unexpected(*args, **kwargs):
        raise AssertionError("no snapshot/PUT in skip-upload path")

    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", pack)
    monkeypatch.setattr(pack_upload_snapshot, "freeze_pack_snapshot", unexpected)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", unexpected)
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)
    assert (
        cli._cmd_pack(
            "2026-08",
            output=output,
            local_parquets=None,
            memory_limit_gb=None,
            skip_upload=True,
        )
        == 0
    )


def test_pack_cli_freeze_failure_prevents_put(monkeypatch, tmp_path):
    output = tmp_path / "companies.zip"

    def pack(*args, **kwargs):
        output.write_bytes(b"PACK")
        return {"count": 1, "size_bytes": 4}

    def fail(*args, **kwargs):
        raise OSError("ENOSPC synthetic")

    def unexpected(*args, **kwargs):
        raise AssertionError("PUT must not start after a local failure")

    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", pack)
    monkeypatch.setattr(pack_upload_snapshot, "freeze_pack_snapshot", fail)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", unexpected)
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic")
    assert (
        cli._cmd_pack(
            "2026-08",
            output=output,
            local_parquets=None,
            memory_limit_gb=None,
            skip_upload=False,
        )
        == 1
    )


@pytest.mark.parametrize("size", [0, -1, True, 8])
def test_freeze_rejects_invalid_size(tmp_path, size):
    output = tmp_path / "companies.zip"
    output.write_bytes(b"PACK")
    with ExitStack() as stack, pytest.raises(ValueError):
        pack_upload_snapshot.freeze_pack_snapshot(stack, output, size)


def test_freeze_rejects_missing_file(tmp_path):
    with ExitStack() as stack, pytest.raises(FileNotFoundError):
        pack_upload_snapshot.freeze_pack_snapshot(stack, tmp_path / "missing.zip", 4)


def test_freeze_rejects_same_size_rewrite_during_copy(monkeypatch, tmp_path):
    output = tmp_path / "companies.zip"
    output.write_bytes(b"AAAAAAAA")
    real = tempfile.TemporaryFile

    class MutateOnWrite:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            self.handle.__enter__()
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def write(self, chunk):
            output.write_bytes(b"BBBBBBBB")
            return self.handle.write(chunk)

        def __getattr__(self, name):
            return getattr(self.handle, name)

    monkeypatch.setattr(
        pack_upload_snapshot.tempfile,
        "TemporaryFile",
        lambda **kwargs: MutateOnWrite(real(**kwargs)),
    )
    with (
        ExitStack() as stack,
        pytest.raises(ValueError, match="changed while freezing"),
    ):
        pack_upload_snapshot.freeze_pack_snapshot(stack, output, 8)


def test_freeze_rejects_inode_swap_during_copy(monkeypatch, tmp_path):
    output = tmp_path / "companies.zip"
    output.write_bytes(b"AAAAAAAA")
    real = tempfile.TemporaryFile

    class ReplaceOnWrite:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            self.handle.__enter__()
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def write(self, chunk):
            replacement = tmp_path / "replacement.zip"
            replacement.write_bytes(b"BBBBBBBB")
            os.replace(replacement, output)
            return self.handle.write(chunk)

        def __getattr__(self, name):
            return getattr(self.handle, name)

    monkeypatch.setattr(
        pack_upload_snapshot.tempfile,
        "TemporaryFile",
        lambda **kwargs: ReplaceOnWrite(real(**kwargs)),
    )
    with ExitStack() as stack, pytest.raises(ValueError, match="path replaced"):
        pack_upload_snapshot.freeze_pack_snapshot(stack, output, 8)
