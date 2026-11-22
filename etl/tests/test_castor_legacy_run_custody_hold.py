"""Regression: the public legacy 'run' cannot bypass raw S/R/F custody (#433).

No RFB, Internet Archive or manifest publication is performed. Test-only
patches below forbid I/O, not bypass the authorization guard.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from ficha_etl import cli


def _forbid_egress(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    effects: list[str] = []

    def forbidden(*_args: object, **_kwargs: object) -> None:
        effects.append("unexpected-work")
        raise AssertionError("legacy HOLD must occur before work, not just upload")

    monkeypatch.setattr(cli, "make_progress", forbidden)
    monkeypatch.setattr(cli.transform, "transform_snapshot", forbidden)
    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", forbidden)
    monkeypatch.setattr(cli.upload_mod, "stream_raw_zips_to_ia", forbidden)
    monkeypatch.setattr(cli.upload_mod, "upload_outputs", forbidden)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", forbidden)
    monkeypatch.setattr(cli.manifest_mod, "update_manifest", forbidden)
    return effects


@pytest.mark.parametrize("credentials", ["missing", "present"])
def test_legacy_run_holds_before_work_and_derived_upload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    credentials: str,
) -> None:
    if credentials == "present":
        monkeypatch.setenv("IA_ACCESS_KEY", "synthetic")
        monkeypatch.setenv("IA_SECRET_KEY", "synthetic")
    else:
        monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
        monkeypatch.delenv("IA_SECRET_KEY", raising=False)

    effects = _forbid_egress(monkeypatch)
    manifest = tmp_path / "manifest.json"
    original = b'{"current":"previous"}\n'
    manifest.write_bytes(original)
    rc = cli._cmd_run(
        "2026-07",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        manifest_path=manifest,
        skip_upload=False,
        verify=False,
        verify_sample_size=1,
    )
    assert rc == 1
    assert "HOLD #433" in capsys.readouterr().err
    assert effects == []
    assert manifest.read_bytes() == original
    assert not (tmp_path / "output").exists()


def test_public_cli_run_without_skip_upload_is_not_an_escape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic")
    effects = _forbid_egress(monkeypatch)

    rc = cli.main(
        [
            "run",
            "--month",
            "2026-07",
            "--cache-dir",
            str(tmp_path / "cache"),
            "--output",
            str(tmp_path / "output"),
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--no-verify",
        ]
    )
    assert rc == 1
    assert "HOLD #433" in capsys.readouterr().err
    assert effects == []
    assert not (tmp_path / "manifest.json").exists()


@pytest.mark.parametrize("credentials", ["missing", "present"])
def test_standalone_pack_holds_before_build_or_upload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    credentials: str,
) -> None:
    if credentials == "present":
        monkeypatch.setenv("IA_ACCESS_KEY", "synthetic")
        monkeypatch.setenv("IA_SECRET_KEY", "synthetic")
    else:
        monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
        monkeypatch.delenv("IA_SECRET_KEY", raising=False)
    effects = _forbid_egress(monkeypatch)
    output = tmp_path / "companies.zip"
    rc = cli._cmd_pack(
        "2026-07",
        output=output,
        local_parquets=None,
        memory_limit_gb=None,
        skip_upload=False,
    )
    assert rc == 1
    assert "HOLD #433" in capsys.readouterr().err
    assert effects == []
    assert not output.exists()


def test_public_pack_command_is_not_an_escape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    effects = _forbid_egress(monkeypatch)
    rc = cli.main(["pack", "--month", "2026-07", "--output", str(tmp_path / "pack.zip")])
    assert rc == 1
    assert "HOLD #433" in capsys.readouterr().err
    assert effects == []
    assert not (tmp_path / "pack.zip").exists()


def test_legacy_run_guard_survives_python_optimized_mode(tmp_path: Path) -> None:
    script = (
        "from pathlib import Path\n"
        "from ficha_etl.cli import _cmd_run\n"
        "exit(_cmd_run('2026-07', cache_dir=Path('.'), "
        "output_dir=Path('unused'), manifest_path=Path('unused.json'), "
        "skip_upload=False, verify=False, verify_sample_size=1))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-O", "-c", script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 1, (proc.stdout, proc.stderr)
    assert "HOLD #433" in proc.stderr
    assert not (tmp_path / "unused.json").exists()
