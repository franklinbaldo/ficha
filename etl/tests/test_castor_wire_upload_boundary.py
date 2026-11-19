"""P0-A: a manifest-only guard is too late to prevent uploading 1.1 wire.

All inputs are tiny offline fixtures; no credentials, network or IA requests.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from ficha_etl import monthly_sharded
from ficha_etl.shard_publish import ShardPublishError


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        month="2026-08",
        prefix="00",
        descriptor=tmp_path / "descriptor-never-read.json",
        receipt=tmp_path / "receipt-never-read.json",
        shard_path=tmp_path / "shard-never-read.zip",
        summary=tmp_path / "summary-never-created.json",
    )


@pytest.mark.parametrize(
    ("wire_version", "shard_mapping"),
    [
        ("1.1.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        ("1.1.0", "cnpj_base_prefix_2"),
        ("1.0.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
    ],
)
def test_submit_refuses_incompatible_wire_before_any_remote_access(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    wire_version: str,
    shard_mapping: str,
) -> None:
    args = _args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", wire_version)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", shard_mapping)

    def no_io(*_args: object, **_kwargs: object) -> None:
        pytest.fail("unversioned 1.1 submission must fail before IO/network")

    for name in (
        "_load_descriptor",
        "_load_receipt",
        "submit_prepared_shard",
        "fetch_item_metadata",
        "fetch_remote_shard_meta",
        "_shard_uploader",
    ):
        monkeypatch.setattr(monthly_sharded, name, no_io)

    with pytest.raises(ShardPublishError, match="unversioned companies-NN.zip"):
        monthly_sharded.command_submit_shard(args)

    assert not args.summary.exists()


def test_unchanged_legacy_wire_passes_only_the_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", "1.0.0")
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", "cnpj_base_prefix_2")

    class ReachedDescriptor(Exception):
        pass

    def stop_before_io(*_args: object, **_kwargs: object) -> None:
        raise ReachedDescriptor

    monkeypatch.setattr(monthly_sharded, "_load_descriptor", stop_before_io)
    with pytest.raises(ReachedDescriptor):
        monthly_sharded.command_submit_shard(args)

    assert not args.summary.exists()


def _monthly_args(tmp_path: Path, *, skip_raw_upload: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        month="2026-08",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        descriptor=tmp_path / "descriptor.json",
        skip_raw_upload=skip_raw_upload,
        no_verify=False,
        verify_sample_size=10,
    )


@pytest.mark.parametrize(
    ("wire_version", "shard_mapping"),
    [
        ("1.1.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        ("1.1.0", "cnpj_base_prefix_2"),
        ("1.0.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
    ],
)
def test_monthly_produce_refuses_wire_before_raw_upload_or_transform(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    wire_version: str,
    shard_mapping: str,
) -> None:
    args = _monthly_args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", wire_version)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", shard_mapping)

    observed_io: list[str] = []

    def forbid(label: str):
        def forbidden(*_args: object, **_kwargs: object) -> None:
            observed_io.append(label)
            pytest.fail(f"produce crossed IO boundary before wire preflight: {label}")

        return forbidden

    monkeypatch.setattr(monthly_sharded, "_require_secret", forbid("secrets"))
    monkeypatch.setattr(monthly_sharded.upload, "stream_raw_zips_to_ia", forbid("raw IA PUT"))
    monkeypatch.setattr(monthly_sharded.transform, "transform_snapshot", forbid("transform"))
    monkeypatch.setattr(
        monthly_sharded, "build_production_descriptor", forbid("descriptor construction")
    )
    monkeypatch.setattr(monthly_sharded, "write_production_descriptor", forbid("descriptor write"))

    with pytest.raises(ShardPublishError, match="unversioned companies-NN.zip"):
        monthly_sharded.command_produce(args)
    assert observed_io == []
    assert not args.descriptor.exists()
    assert not args.output_dir.exists()


@pytest.mark.parametrize(
    ("wire_version", "shard_mapping"),
    [
        ("1.1.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        ("1.1.0", "cnpj_base_prefix_2"),
        ("1.0.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
    ],
)
def test_monthly_output_upload_refuses_wire_before_descriptor_or_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    wire_version: str,
    shard_mapping: str,
) -> None:
    args = _monthly_args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", wire_version)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", shard_mapping)

    observed_io: list[str] = []

    def forbid(label: str):
        def forbidden(*_args: object, **_kwargs: object) -> None:
            observed_io.append(label)
            pytest.fail(f"upload-outputs crossed IO boundary before wire preflight: {label}")

        return forbidden

    monkeypatch.setattr(monthly_sharded, "_load_descriptor", forbid("descriptor read"))
    monkeypatch.setattr(monthly_sharded, "_require_secret", forbid("secrets"))
    monkeypatch.setattr(monthly_sharded, "submit_outputs_fail_closed", forbid("Parquet IA PUT"))

    with pytest.raises(ShardPublishError, match="unversioned companies-NN.zip"):
        monthly_sharded.command_upload_outputs(args)
    assert observed_io == []


def test_monthly_dry_run_still_reaches_transform_under_v11(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _monthly_args(tmp_path, skip_raw_upload=True)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", "1.1.0")
    monkeypatch.setattr(
        monthly_sharded, "COMPANIES_SHARD_BY", "cnpj_base_prefix_or_base36_mod_100_v1"
    )

    class ReachedTransform(Exception):
        pass

    passed_flags: list[bool] = []

    def stop_at_transform(*_args: object, **kwargs: object) -> None:
        passed_flags.append(kwargs.get("reject_empty_estabelecimentos") is True)
        raise ReachedTransform

    def no_upload(*_args: object, **_kwargs: object) -> None:
        pytest.fail("skip-raw-upload must not access secrets or trigger IA PUT")

    monkeypatch.setattr(monthly_sharded, "_require_secret", no_upload)
    monkeypatch.setattr(monthly_sharded.upload, "stream_raw_zips_to_ia", no_upload)
    monkeypatch.setattr(monthly_sharded.transform, "transform_snapshot", stop_at_transform)
    with pytest.raises(ReachedTransform):
        monthly_sharded.command_produce(args)
    assert passed_flags == [True]


@pytest.mark.parametrize("part", [0, 1, 5, 9])
def test_monthly_invalid_source_aborts_before_raw_put_or_descriptor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, part: int
) -> None:
    # Real zero-byte ZIP-member detection is exercised in test_transform.py.
    # Here the monthly entry point must keep every remote effect after it.
    args = _monthly_args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", "1.0.0")
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", "cnpj_base_prefix_2")

    calls: list[str] = []

    def reject_source(*_args: object, **kwargs: object) -> None:
        calls.append("transform")
        assert kwargs["reject_empty_estabelecimentos"] is True
        raise RuntimeError(f"Estabelecimentos{part}.zip: empty member")

    def forbid(*_args: object, **_kwargs: object) -> None:
        pytest.fail("raw upload, secrets, or descriptor reached after source failure")

    monkeypatch.setattr(monthly_sharded.transform, "transform_snapshot", reject_source)
    monkeypatch.setattr(monthly_sharded, "_require_secret", forbid)
    monkeypatch.setattr(monthly_sharded.upload, "stream_raw_zips_to_ia", forbid)
    monkeypatch.setattr(monthly_sharded, "build_production_descriptor", forbid)
    monkeypatch.setattr(monthly_sharded, "write_production_descriptor", forbid)

    with pytest.raises(RuntimeError, match=f"Estabelecimentos{part}.zip"):
        monthly_sharded.command_produce(args)
    assert calls == ["transform"]
    assert not args.descriptor.exists()


def test_monthly_legacy_wire_transforms_before_raw_upload_and_descriptor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _monthly_args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", "1.0.0")
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", "cnpj_base_prefix_2")

    calls: list[str] = []

    def transform_first(*_args: object, **kwargs: object) -> None:
        assert kwargs["reject_empty_estabelecimentos"] is True
        calls.append("transform")

    def fake_secret(name: str) -> str:
        calls.append(f"secret:{name}")
        return "test-only"

    def upload_after_transform(*_args: object, **_kwargs: object) -> None:
        calls.append("raw")

    def build_after_upload(*_args: object, **_kwargs: object) -> dict:
        calls.append("descriptor")
        return {}

    def write_last(*_args: object, **_kwargs: object) -> None:
        calls.append("write")

    monkeypatch.setattr(monthly_sharded.transform, "transform_snapshot", transform_first)
    monkeypatch.setattr(monthly_sharded, "run_monthly_gate", lambda *_args: calls.append("gate"))
    monkeypatch.setattr(monthly_sharded, "_require_secret", fake_secret)
    monkeypatch.setattr(monthly_sharded.upload, "stream_raw_zips_to_ia", upload_after_transform)
    monkeypatch.setattr(monthly_sharded, "build_production_descriptor", build_after_upload)
    monkeypatch.setattr(monthly_sharded, "write_production_descriptor", write_last)

    monthly_sharded.command_produce(args)
    assert calls == [
        "transform",
        "gate",
        "secret:IA_ACCESS_KEY",
        "secret:IA_SECRET_KEY",
        "raw",
        "descriptor",
        "write",
    ]


def test_monthly_legacy_wire_still_reaches_output_descriptor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _monthly_args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", "1.0.0")
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", "cnpj_base_prefix_2")

    class ReachedDescriptor(Exception):
        pass

    def stop_at_descriptor(*_args: object, **_kwargs: object) -> None:
        raise ReachedDescriptor

    monkeypatch.setattr(monthly_sharded, "_load_descriptor", stop_at_descriptor)
    with pytest.raises(ReachedDescriptor):
        monthly_sharded.command_upload_outputs(args)
