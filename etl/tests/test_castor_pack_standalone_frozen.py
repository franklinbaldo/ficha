"""Offline regression for standalone `ficha-etl pack` ZIP snapshot (#361)."""

from __future__ import annotations

import hashlib
import os
import tempfile
from contextlib import ExitStack
from types import SimpleNamespace

import pytest

from ficha_etl import cli, pack, pack_upload_snapshot


@pytest.fixture(autouse=True)
def _isolate_legacy_packer_bytes_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests isolate historical ZIP snapshot behavior, not raw custody."""
    monkeypatch.setattr(cli, "_require_legacy_publication_custody", lambda: None)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("mutation", ["rewrite", "replace"])
def test_pack_cli_passes_anonymous_bytes_even_if_path_changes(monkeypatch, tmp_path, mutation):
    output = tmp_path / "companies.zip"
    original = b"PACKED ORIGINAL"
    events = []

    def pack(*args, **kwargs):
        output.write_bytes(original)
        events.append("pack")
        return {"count": 1, "size_bytes": len(original), "zip_sha256": _sha(original)}

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
        return {"count": 1, "size_bytes": 4, "zip_sha256": _sha(b"PACK")}

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
        return {"count": 1, "size_bytes": 4, "zip_sha256": _sha(b"PACK")}

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
        pack_upload_snapshot.freeze_pack_snapshot(
            stack, output, size, expected_sha256=_sha(b"PACK")
        )


def test_freeze_rejects_missing_file(tmp_path):
    with ExitStack() as stack, pytest.raises(FileNotFoundError):
        pack_upload_snapshot.freeze_pack_snapshot(
            stack, tmp_path / "missing.zip", 4, expected_sha256=_sha(b"PACK")
        )


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
        pack_upload_snapshot.freeze_pack_snapshot(
            stack, output, 8, expected_sha256=_sha(b"AAAAAAAA")
        )


@pytest.mark.skipif(os.name == "nt", reason="Windows locks opened files against os.replace")
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
    # Windows holds the source inode with sharing flags that can deny a
    # replacement while it is open. That is also a fail-closed outcome.
    if os.name == "nt":
        with ExitStack() as stack, pytest.raises(PermissionError):
            pack_upload_snapshot.freeze_pack_snapshot(
                stack, output, 8, expected_sha256=_sha(b"AAAAAAAA")
            )
    else:
        with ExitStack() as stack, pytest.raises(ValueError, match="path replaced"):
            pack_upload_snapshot.freeze_pack_snapshot(
                stack, output, 8, expected_sha256=_sha(b"AAAAAAAA")
            )


def test_freeze_rejects_simulated_inode_swap_on_all_platforms(monkeypatch, tmp_path):
    """Exercise the path-identity guard without relying on OS rename semantics.

    Windows denies os.replace while the source file is open. The POSIX
    test above separately exercises a real pathname swap.
    """
    output = tmp_path / "companies.zip"
    output.write_bytes(b"AAAAAAAA")
    original_stat = type(output).stat
    real = tempfile.TemporaryFile
    writing = False

    class SimulateSwapOnWrite:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            self.handle.__enter__()
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def write(self, chunk):
            nonlocal writing
            writing = True
            return self.handle.write(chunk)

        def __getattr__(self, name):
            return getattr(self.handle, name)

    def stat_after_swap(path, *args, **kwargs):
        current = original_stat(path, *args, **kwargs)
        if path == output and writing:
            return SimpleNamespace(st_dev=current.st_dev, st_ino=current.st_ino + 1)
        return current

    monkeypatch.setattr(type(output), "stat", stat_after_swap)
    monkeypatch.setattr(
        pack_upload_snapshot.tempfile,
        "TemporaryFile",
        lambda **kwargs: SimulateSwapOnWrite(real(**kwargs)),
    )
    with ExitStack() as stack, pytest.raises(ValueError, match="path replaced"):
        pack_upload_snapshot.freeze_pack_snapshot(
            stack, output, 8, expected_sha256=_sha(b"AAAAAAAA")
        )


def test_producer_attests_writer_inode_bytes(tmp_path):
    output = tmp_path / "companies.zip"
    result = pack.pack_companies(
        iter(()),
        {kind: [] for kind in pack.LOOKUP_KINDS},
        output,
        "2026-08",
        attest_output=True,
    )
    assert result["zip_sha256"] == _sha(output.read_bytes())
    assert result["size_bytes"] == output.stat().st_size


def test_pack_cli_rejects_same_size_swap_before_freeze(monkeypatch, tmp_path, capsys):
    output = tmp_path / "companies.zip"
    original = b"PACKED ORIGINAL"
    uploaded = []

    def fake_packer(*args, **kwargs):
        assert kwargs["attest_output"] is True
        output.write_bytes(original)
        digest = _sha(original)
        output.write_bytes(b"X" * len(original))
        return {"count": 1, "size_bytes": len(original), "zip_sha256": digest}

    def no_put(*args, **kwargs):
        uploaded.append(True)
        raise AssertionError("PUT must never happen for mismatching producer digest")

    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", fake_packer)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", no_put)
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
    assert "producer SHA-256" in capsys.readouterr().err
    assert uploaded == []


def test_pack_cli_missing_producer_digest_fails_closed(monkeypatch, tmp_path):
    output = tmp_path / "companies.zip"

    def legacy_packer(*args, **kwargs):
        output.write_bytes(b"PACK")
        return {"count": 1, "size_bytes": 4}

    def no_put(*args, **kwargs):
        raise AssertionError("missing producer digest must prevent PUT")

    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", legacy_packer)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", no_put)
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


@pytest.mark.parametrize("bad_digest", ["", "0" * 63, "A" * 64, None])
def test_freeze_rejects_invalid_producer_digest(tmp_path, bad_digest):
    output = tmp_path / "companies.zip"
    output.write_bytes(b"PACK")
    with ExitStack() as stack, pytest.raises(ValueError, match="invalid ZIP SHA-256"):
        pack_upload_snapshot.freeze_pack_snapshot(stack, output, 4, expected_sha256=bad_digest)


def test_freeze_rejects_wrong_producer_digest(tmp_path):
    output = tmp_path / "companies.zip"
    output.write_bytes(b"PACK")
    with ExitStack() as stack, pytest.raises(ValueError, match="producer SHA-256"):
        pack_upload_snapshot.freeze_pack_snapshot(stack, output, 4, expected_sha256=_sha(b"FAIL"))


@pytest.mark.skipif(os.name == "nt", reason="Windows denies replacing an open ZIP writer inode")
def test_pack_producer_rejects_part_path_swap_before_rename(monkeypatch, tmp_path):
    output = tmp_path / "companies.zip"
    part = tmp_path / "companies.zip.part"
    original_zip_class = pack.SpoolingZipFile

    class SwapAfterClose(original_zip_class):
        def close(self):
            was_open = self.fp is not None
            super().close()
            if was_open:
                replacement = tmp_path / "replacement.part"
                replacement.write_bytes(b"X" * part.stat().st_size)
                os.replace(replacement, part)

    monkeypatch.setattr(pack, "SpoolingZipFile", SwapAfterClose)
    with pytest.raises(ValueError, match="changed during producer attestation"):
        pack.pack_companies(
            iter(()),
            {kind: [] for kind in pack.LOOKUP_KINDS},
            output,
            "2026-08",
            attest_output=True,
        )
    assert not output.exists()
