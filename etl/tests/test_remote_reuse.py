"""Testes do reconhecimento de outputs derivados duráveis (#132 slice 1).

Nenhum acesso a rede: o metadata do item é injetado como dict e a sonda de
Parquet é injetada como callable. O único I/O é em arquivos temporários locais,
usados para provar que a checagem de footer rejeita um Parquet truncado de
verdade — e não só um mock que finge falhar.
"""

from __future__ import annotations

import duckdb
import pytest

from ficha_etl.remote_reuse import (
    REQUIRED_COLUMNS,
    ParquetProbeResult,
    ReuseState,
    all_outputs_reusable,
    classify_outputs,
    duckdb_parquet_probe,
    format_verdicts,
)

MONTH = "2026-05"


def _metadata(files: list[dict]) -> dict:
    return {"files": files}


def _full_metadata(size: int = 1024) -> dict:
    return _metadata([{"name": name, "size": str(size)} for name in REQUIRED_COLUMNS])


def _probe_ok(url: str) -> ParquetProbeResult:
    """Sonda que devolve sempre um artefato saudável para o nome pedido."""
    name = url.split(f"ficha-{MONTH}/", 1)[-1]
    expected = REQUIRED_COLUMNS[name] or frozenset()
    return ParquetProbeResult(columns=expected | {"coluna_extra"}, row_count=42)


# --- caminho feliz -----------------------------------------------------------


def test_full_materialization_is_reusable() -> None:
    verdicts = classify_outputs(MONTH, metadata=_full_metadata(), probe=_probe_ok)
    assert all_outputs_reusable(verdicts)
    assert {v.state for v in verdicts.values()} == {ReuseState.REUSABLE}
    assert verdicts["cnpjs.parquet"].row_count == 42


def test_verdicts_cover_the_whole_contract() -> None:
    verdicts = classify_outputs(MONTH, metadata=_full_metadata(), probe=_probe_ok)
    assert set(verdicts) == set(REQUIRED_COLUMNS)
    assert "companies.zip" not in verdicts, "slice 3 está bloqueado por #133"


# --- os três resultados conceituais ------------------------------------------


def test_absent_file_is_absent_and_forces_recompute() -> None:
    files = [{"name": n, "size": "1024"} for n in REQUIRED_COLUMNS if n != "raizes.parquet"]
    verdicts = classify_outputs(MONTH, metadata=_metadata(files), probe=_probe_ok)
    assert verdicts["raizes.parquet"].state is ReuseState.ABSENT
    assert verdicts["raizes.parquet"].must_recompute
    assert not all_outputs_reusable(verdicts)


def test_zero_byte_file_is_invalid() -> None:
    files = [
        {"name": n, "size": "0" if n == "socios.parquet" else "1024"} for n in REQUIRED_COLUMNS
    ]
    verdicts = classify_outputs(MONTH, metadata=_metadata(files), probe=_probe_ok)
    assert verdicts["socios.parquet"].state is ReuseState.INVALID
    assert "size zero" in verdicts["socios.parquet"].detail


def test_unreadable_size_is_invalid_not_absent() -> None:
    """Entrada existe mas o metadata veio parcial — ambíguo, não ausente."""
    files = [
        {"name": n, **({} if n == "pessoas.parquet" else {"size": "1024"})}
        for n in REQUIRED_COLUMNS
    ]
    verdicts = classify_outputs(MONTH, metadata=_metadata(files), probe=_probe_ok)
    assert verdicts["pessoas.parquet"].state is ReuseState.INVALID
    assert "size ilegível" in verdicts["pessoas.parquet"].detail


def test_wrong_schema_is_invalid() -> None:
    def probe(url: str) -> ParquetProbeResult:
        if url.endswith("cnpj_cnaes.parquet"):
            return ParquetProbeResult(columns=frozenset({"algo_diferente"}), row_count=10)
        return _probe_ok(url)

    verdicts = classify_outputs(MONTH, metadata=_full_metadata(), probe=probe)
    verdict = verdicts["cnpj_cnaes.parquet"]
    assert verdict.state is ReuseState.INVALID
    assert "colunas ausentes" in verdict.detail
    assert "cnae_codigo" in verdict.detail


def test_empty_parquet_is_invalid() -> None:
    def probe(url: str) -> ParquetProbeResult:
        base = _probe_ok(url)
        if url.endswith("enderecos.parquet"):
            return ParquetProbeResult(columns=base.columns, row_count=0)
        return base

    verdicts = classify_outputs(MONTH, metadata=_full_metadata(), probe=probe)
    assert verdicts["enderecos.parquet"].state is ReuseState.INVALID
    assert "row_count zero" in verdicts["enderecos.parquet"].detail


def test_probe_failure_is_invalid_not_an_exception() -> None:
    """Corrupção não pode virar erro irrecuperável — o pipeline sabe reconstruir."""

    def probe(url: str) -> ParquetProbeResult:
        if url.endswith("cnpjs.parquet"):
            raise OSError("Invalid Input Error: file is not a valid Parquet file")
        return _probe_ok(url)

    verdicts = classify_outputs(MONTH, metadata=_full_metadata(), probe=probe)
    assert verdicts["cnpjs.parquet"].state is ReuseState.INVALID
    assert "footer ilegível" in verdicts["cnpjs.parquet"].detail


def test_metadata_unavailable_forces_recompute_without_raising() -> None:
    verdicts = classify_outputs(MONTH, metadata=None, probe=_probe_ok)
    assert set(verdicts) == set(REQUIRED_COLUMNS)
    assert all(v.state is ReuseState.INVALID for v in verdicts.values())
    assert all(v.must_recompute for v in verdicts.values())
    assert not all_outputs_reusable(verdicts)


def test_partial_metadata_response_yields_no_false_reuse() -> None:
    """Resposta transitória sem a chave `files` não pode virar reuse."""
    verdicts = classify_outputs(MONTH, metadata={"metadata": {"identifier": "ficha-2026-05"}})
    assert all(v.state is ReuseState.ABSENT for v in verdicts.values())
    assert not all_outputs_reusable(verdicts)


# --- presença sozinha nunca basta -------------------------------------------


def test_presence_alone_does_not_authorize_reuse() -> None:
    """`remote exists => skip` é exatamente o que este slice não pode fazer."""

    def probe(url: str) -> ParquetProbeResult:
        raise ValueError("footer ausente")

    verdicts = classify_outputs(MONTH, metadata=_full_metadata(size=10**9), probe=probe)
    parquets = {n for n, cols in REQUIRED_COLUMNS.items() if cols is not None}
    assert all(verdicts[n].state is ReuseState.INVALID for n in parquets)


# --- a decisão não pode depender do manifesto promovido ----------------------


def test_reuse_decision_never_reads_the_promoted_manifest(monkeypatch) -> None:
    """Sem circularidade: o sha256 do manifest só existe depois da promoção."""
    import ficha_etl.manifest as manifest_mod

    def explode(*args, **kwargs):
        raise AssertionError("classify_outputs não pode depender do manifest promovido")

    monkeypatch.setattr(manifest_mod, "build_snapshot_entry", explode)
    monkeypatch.setattr(manifest_mod, "verify_snapshot_files", explode)

    verdicts = classify_outputs(MONTH, metadata=_full_metadata(), probe=_probe_ok)
    assert all_outputs_reusable(verdicts)


# --- footer real, com arquivo truncado de verdade ----------------------------


def _write_parquet(path, rows: int) -> None:
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT i::VARCHAR AS codigo, 'x' AS descricao FROM range({rows}) t(i)) "
            f"TO '{path}' (FORMAT PARQUET)"
        )
    finally:
        con.close()


def test_real_probe_reads_valid_parquet(tmp_path) -> None:
    path = tmp_path / "lookup.parquet"
    _write_parquet(path, rows=7)
    result = duckdb_parquet_probe(str(path))
    assert result.row_count == 7
    assert {"codigo", "descricao"} <= result.columns


def test_real_probe_rejects_truncated_parquet(tmp_path) -> None:
    """Prova que a checagem de footer pega truncamento real, não só um mock."""
    path = tmp_path / "lookup.parquet"
    _write_parquet(path, rows=1000)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])  # footer vai embora

    with pytest.raises(Exception):  # noqa: B017 — DuckDB varia o tipo por versão
        duckdb_parquet_probe(str(path))


def test_truncated_parquet_classifies_as_invalid(tmp_path) -> None:
    """O truncamento real atravessa a classificação como INVALID."""
    path = tmp_path / "cnpjs.parquet"
    _write_parquet(path, rows=1000)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])

    def local_probe(url: str) -> ParquetProbeResult:
        return duckdb_parquet_probe(str(path))

    verdicts = classify_outputs(
        MONTH, metadata=_metadata([{"name": "cnpjs.parquet", "size": "500"}]), probe=local_probe
    )
    assert verdicts["cnpjs.parquet"].state is ReuseState.INVALID
    assert verdicts["cnpjs.parquet"].must_recompute


# --- observabilidade ---------------------------------------------------------


def test_format_verdicts_emits_one_factual_line_per_artifact() -> None:
    verdicts = classify_outputs(MONTH, metadata=_full_metadata(), probe=_probe_ok)
    lines = format_verdicts(verdicts).splitlines()
    assert len(lines) == len(REQUIRED_COLUMNS)
    assert all("reusable" in line for line in lines)


def test_classify_outputs_rejects_bad_month() -> None:
    with pytest.raises(ValueError, match="YYYY-MM"):
        classify_outputs("2026-5", metadata=_full_metadata(), probe=_probe_ok)
