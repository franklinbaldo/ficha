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


@pytest.fixture(autouse=True)
def _isolate_historical_preput_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests isolate prior stage ordering, not publication authorization."""
    monkeypatch.setattr(cli, "_require_legacy_run_custody", lambda: None)


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

    def fail_transform(*_args: object, **kwargs: object) -> None:
        assert kwargs["reject_empty_estabelecimentos"] is True
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


def test_legacy_run_refuses_raw_put_when_local_pack_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A local companies.zip failure must precede *all* remote publication."""
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic-access")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setattr(cli, "make_progress", _Progress)
    events: list[str] = []

    def transform(*_args: object, **kwargs: object) -> None:
        assert kwargs["reject_empty_estabelecimentos"] is True
        events.append("transform")

    def fail_pack(*_args: object, **_kwargs: object) -> None:
        events.append("pack")
        raise RuntimeError("synthetic invalid companies Parquet")

    monkeypatch.setattr(cli.transform, "transform_snapshot", transform)
    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", fail_pack)
    monkeypatch.setattr(cli.upload_mod, "stream_raw_zips_to_ia", _reject)
    monkeypatch.setattr(cli.upload_mod, "upload_outputs", _reject)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", _reject)
    monkeypatch.setattr(cli.manifest_mod, "build_snapshot_entry", _reject)

    assert _run(tmp_path, skip_upload=False) == 1
    assert events == ["transform", "pack"]
    assert not (tmp_path / "manifest.json").exists()


def test_legacy_run_refuses_raw_put_when_local_manifest_build_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Local manifest validation and hashing precede remote publication."""
    monkeypatch.setenv("IA_ACCESS_KEY", "synthetic-access")
    monkeypatch.setenv("IA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setattr(cli, "make_progress", _Progress)
    events: list[str] = []

    def transform(*_args: object, **kwargs: object) -> None:
        assert kwargs["reject_empty_estabelecimentos"] is True
        events.append("transform")

    def pack(*_args: object, **_kwargs: object) -> dict[str, int]:
        events.append("pack")
        return {"count": 1, "size_bytes": 10}

    def fail_manifest(*_args: object, **_kwargs: object) -> None:
        events.append("build")
        raise RuntimeError("synthetic missing local parquet")

    monkeypatch.setattr(cli.transform, "transform_snapshot", transform)
    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", pack)
    monkeypatch.setattr(cli.manifest_mod, "build_snapshot_entry", fail_manifest)
    monkeypatch.setattr(cli.upload_mod, "stream_raw_zips_to_ia", _reject)
    monkeypatch.setattr(cli.upload_mod, "upload_outputs", _reject)
    monkeypatch.setattr(cli.upload_mod, "upload_companies_zip", _reject)
    monkeypatch.setattr(cli.manifest_mod, "update_manifest", _reject)

    assert _run(tmp_path, skip_upload=False) == 1
    assert events == ["transform", "pack", "build"]
    assert not (tmp_path / "manifest.json").exists()


def test_legacy_run_rejects_missing_ia_credentials_before_work(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)
    monkeypatch.setattr(cli, "make_progress", _Progress)
    monkeypatch.setattr(cli.transform, "transform_snapshot", _reject)
    monkeypatch.setattr(cli.pack_mod, "pack_from_parquets", _reject)
    assert _run(tmp_path, skip_upload=False) == 1


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

    def transform(*_args: object, **kwargs: object) -> None:
        assert kwargs["reject_empty_estabelecimentos"] is True
        events.append("transform")

    monkeypatch.setattr(cli.transform, "transform_snapshot", transform)
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
    monkeypatch.setattr(
        cli.legacy_upload_snapshot,
        "freeze_uploads",
        lambda *_: {"companies.zip": object()},
    )

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
            "pack",
            "build",
            "raw",
            "outputs",
            "companies",
            "verify",
            "manifest",
        ]
