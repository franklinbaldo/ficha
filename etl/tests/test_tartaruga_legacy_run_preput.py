"""Offline contract: legacy `ficha-etl run` must transform before raw PUT.

Remote calls and filesystem-heavy stages are faked; no RFB, IA or real snapshot.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ficha_etl import cli


class _Progress:
    def start(self) -> None:
        pass

    def add_task(self, *_args: object, **_kwargs: object) -> int:
        return 0

    def update(self, *_args: object, **_kwargs: object) -> None:
        pass

    def stop(self) -> None:
        pass


def _run(tmp_path: Path, *, skip_upload: bool) -> int:
    return cli._cmd_run(
        "2026-08",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        manifest_path=tmp_path / "manifest.json",
        skip_upload=skip_upload,
        verify=False,
        verify_sample_size=1,
    )


def _reject(*_args: object, **_kwargs: object) -> None:
    pytest.fail("remote/publication action reached unexpectedly")


@pytest.mark.parametrize("problem", ["empty estabelecimento CSV", "bad ZIP member"])
def test_legacy_run_refuses_raw_put_when_transform_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, problem: str
) -> None:
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic-access")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setattr(cli, "make_progress", _Progress)
    events: list[str] = []

    def fail_transform(*_args: object, **_kwargs: object) -> None:
        events.append("transform")
        raise RuntimeError(problem)

    monkeypatch.setattr(cli.transform, "transform_snapshot", fail_transform)
    monkeypatch.setattr(cli.upload_mod, "stream_raw_zips_to_ia", _reject)
    monkeypatch.setattr(cli.upload_mod, "upload_outputs", _reject)
    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", _reject)
    monkeypatch.setattr(cli.manifest_mod, "build_snapshot_entry", _reject)

    assert _run(tmp_path, skip_upload=False) == 1
    assert events == ["transform"]
    assert not (tmp_path / "manifest.json").exists()


@pytest.mark.parametrize("skip_upload", [False, True])
def test_legacy_run_orders_transform_before_every_remote_effect(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, skip_upload: bool
) -> None:
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic-access")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setattr(cli, "make_progress", _Progress)
    events: list[str] = []

    def record(label: str):
        def run(*_args: object, **_kwargs: object) -> None:
            events.append(label)

        return run

    monkeypatch.setattr(cli.transform, "transform_snapshot", record("transform"))
    monkeypatch.setattr(cli.upload_mod, "stream_raw_zips_to_ia", record("raw"))
    monkeypatch.setattr(cli.upload_mod, "upload_outputs", record("outputs"))
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", record("companies"))

    def pack(*_args: object, **_kwargs: object) -> dict[str, int]:
        events.append("pack")
        return {"count": 1, "size_bytes": 10}

    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", pack)

    def build(*_args: object, **_kwargs: object) -> object:
        events.append("build")
        return object()

    monkeypatch.setattr(cli.manifest_mod, "build_snapshot_entry", build)

    def verify(*_args: object, **_kwargs: object) -> list[str]:
        events.append("verify")
        return []

    monkeypatch.setattr(cli.manifest_mod, "verify_snapshot_files", verify)
    monkeypatch.setattr(cli.manifest_mod, "update_manifest", record("manifest"))

    assert _run(tmp_path, skip_upload=skip_upload) == 0
    if skip_upload:
        assert events == ["transform", "pack", "build", "manifest"]
    else:
        assert events == [
            "transform",
            "raw",
            "outputs",
            "pack",
            "companies",
            "build",
            "verify",
            "manifest",
        ]
