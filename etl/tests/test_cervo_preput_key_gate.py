"""Monthly command must reject malformed produced keys before raw IA effects.

Uses tiny Parquet fixtures and fake remote boundaries; no RFB/IA credentials.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pytest

from ficha_etl import monthly_sharded
from ficha_etl.monthly_estabelecimento_gate import MonthlyEstabelecimentoKeyGateError


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        month="2026-08",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        descriptor=tmp_path / "production-descriptor.json",
        skip_raw_upload=False,
        no_verify=False,
        verify_sample_size=10,
    )


def _produced_parquet(path: Path, values: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE rows(cnpj VARCHAR)")
        con.executemany("INSERT INTO rows VALUES (?)", [(v,) for v in values])
        con.execute("COPY rows TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(path)])
    finally:
        con.close()


def _legacy_wire(monkeypatch: pytest.MonkeyPatch) -> None:
    # Production defaults may use wire 1.1, forbidden under legacy shard names.
    # Exercise the *additional* gate after the existing wire preflight passes.
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", "1.0.0")
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", "cnpj_base_prefix_2")


@pytest.mark.parametrize(
    ("values", "message"),
    [
        (["12345678000195", "12345678000195"], "duplicate full estabelecimento key"),
        (["12345678000195", "12345678000196"], "malformed published CNPJ"),
    ],
)
def test_produce_rejects_bad_produced_keys_before_any_raw_put(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    values: list[str],
    message: str,
) -> None:
    args = _args(tmp_path)
    _legacy_wire(monkeypatch)
    calls: list[str] = []

    def transform(*_args: object, **kwargs: object) -> None:
        calls.append("transform")
        assert kwargs["reject_empty_estabelecimentos"] is True
        _produced_parquet(args.output_dir / "cnpjs.parquet", values)

    def forbid(*_args: object, **_kwargs: object) -> None:
        pytest.fail("key validation failed but raw PUT/secrets/descriptor was reached")

    monkeypatch.setattr(monthly_sharded.transform, "transform_snapshot", transform)
    monkeypatch.setattr(monthly_sharded, "_require_secret", forbid)
    monkeypatch.setattr(monthly_sharded.upload, "stream_raw_zips_to_ia", forbid)
    monkeypatch.setattr(monthly_sharded, "build_production_descriptor", forbid)
    monkeypatch.setattr(monthly_sharded, "write_production_descriptor", forbid)

    with pytest.raises(MonthlyEstabelecimentoKeyGateError, match=message):
        monthly_sharded.command_produce(args)

    evidence = tmp_path / "evidence" / "monthly-estabelecimento-key-gate.json"
    assert json.loads(evidence.read_text(encoding="utf-8"))["status"] == "failed"
    assert calls == ["transform"]
    assert not args.descriptor.exists()


def test_produce_valid_keys_pass_gate_before_raw_and_descriptor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _args(tmp_path)
    _legacy_wire(monkeypatch)
    calls: list[str] = []
    evidence = tmp_path / "evidence" / "monthly-estabelecimento-key-gate.json"

    def transform(*_args: object, **_kwargs: object) -> None:
        calls.append("transform")
        _produced_parquet(args.output_dir / "cnpjs.parquet", ["12345678000195"])

    def secret(_name: str) -> str:
        assert json.loads(evidence.read_text(encoding="utf-8"))["status"] == "ok"
        calls.append("secret")
        return "fixture-only"

    def raw(*_args: object, **_kwargs: object) -> None:
        calls.append("raw")

    def build(*_args: object, **_kwargs: object) -> dict:
        calls.append("build")
        return {}

    def write(*_args: object, **_kwargs: object) -> None:
        calls.append("write")

    monkeypatch.setattr(monthly_sharded.transform, "transform_snapshot", transform)
    monkeypatch.setattr(monthly_sharded, "_require_secret", secret)
    monkeypatch.setattr(monthly_sharded.upload, "stream_raw_zips_to_ia", raw)
    monkeypatch.setattr(monthly_sharded, "build_production_descriptor", build)
    monkeypatch.setattr(monthly_sharded, "write_production_descriptor", write)
    monkeypatch.setattr(
        monthly_sharded,
        "_bind_source_receipt",
        lambda *_args: calls.append("source-attestation"),
    )

    monthly_sharded.command_produce(args)
    assert calls == ["transform", "build", "source-attestation", "write", "secret", "secret", "raw"]
