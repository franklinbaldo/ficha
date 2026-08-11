"""Testes do reconhecimento de outputs derivados duráveis (#132 slice 1).

Nenhum acesso a rede: o metadata do item é injetado como dict e a sonda de
Parquet é injetada como callable. O único I/O é em arquivos temporários locais,
usados para provar que a checagem de footer rejeita um Parquet truncado de
verdade — e não só um mock que finge falhar.
"""

from __future__ import annotations

import duckdb
import pytest

import httpx

from ficha_etl.remote_reuse import (
    LOOKUPS_JSON_REQUIRED_KEYS,
    REQUIRED_COLUMNS,
    ParquetProbeResult,
    ReuseState,
    all_outputs_reusable,
    classify_outputs,
    duckdb_parquet_probe,
    fetch_item_metadata,
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


def _lookups_ok(url: str) -> object:
    return {key: {} for key in LOOKUPS_JSON_REQUIRED_KEYS}


def _classify(metadata, probe=_probe_ok, json_fetch=_lookups_ok):
    return classify_outputs(MONTH, metadata=metadata, probe=probe, json_fetch=json_fetch)


# --- caminho feliz -----------------------------------------------------------


def test_full_materialization_is_reusable() -> None:
    verdicts = _classify(_full_metadata())
    assert all_outputs_reusable(verdicts)
    assert {v.state for v in verdicts.values()} == {ReuseState.REUSABLE}
    assert verdicts["cnpjs.parquet"].row_count == 42


def test_verdicts_cover_the_whole_contract() -> None:
    verdicts = _classify(_full_metadata())
    assert set(verdicts) == set(REQUIRED_COLUMNS)
    assert "companies.zip" not in verdicts, "slice 3 está bloqueado por #133"


# --- os três resultados conceituais ------------------------------------------


def test_absent_file_is_absent_and_forces_recompute() -> None:
    files = [{"name": n, "size": "1024"} for n in REQUIRED_COLUMNS if n != "raizes.parquet"]
    verdicts = _classify(_metadata(files))
    assert verdicts["raizes.parquet"].state is ReuseState.ABSENT
    assert verdicts["raizes.parquet"].must_recompute
    assert not all_outputs_reusable(verdicts)


def test_zero_byte_file_is_invalid() -> None:
    files = [
        {"name": n, "size": "0" if n == "socios.parquet" else "1024"} for n in REQUIRED_COLUMNS
    ]
    verdicts = _classify(_metadata(files))
    assert verdicts["socios.parquet"].state is ReuseState.INVALID
    assert "size zero" in verdicts["socios.parquet"].detail


def test_unreadable_size_is_invalid_not_absent() -> None:
    """Entrada existe mas o metadata veio parcial — ambíguo, não ausente."""
    files = [
        {"name": n, **({} if n == "pessoas.parquet" else {"size": "1024"})}
        for n in REQUIRED_COLUMNS
    ]
    verdicts = _classify(_metadata(files))
    assert verdicts["pessoas.parquet"].state is ReuseState.INVALID
    assert "size ilegível" in verdicts["pessoas.parquet"].detail


def test_wrong_schema_is_invalid() -> None:
    def probe(url: str) -> ParquetProbeResult:
        if url.endswith("cnpj_cnaes.parquet"):
            return ParquetProbeResult(columns=frozenset({"algo_diferente"}), row_count=10)
        return _probe_ok(url)

    verdicts = _classify(_full_metadata(), probe=probe)
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

    verdicts = _classify(_full_metadata(), probe=probe)
    assert verdicts["enderecos.parquet"].state is ReuseState.INVALID
    assert "row_count zero" in verdicts["enderecos.parquet"].detail


def test_probe_failure_is_invalid_not_an_exception() -> None:
    """Corrupção não pode virar erro irrecuperável — o pipeline sabe reconstruir."""

    def probe(url: str) -> ParquetProbeResult:
        if url.endswith("cnpjs.parquet"):
            raise OSError("Invalid Input Error: file is not a valid Parquet file")
        return _probe_ok(url)

    verdicts = _classify(_full_metadata(), probe=probe)
    assert verdicts["cnpjs.parquet"].state is ReuseState.INVALID
    assert "footer ilegível" in verdicts["cnpjs.parquet"].detail


def test_metadata_unavailable_forces_recompute_without_raising() -> None:
    verdicts = _classify(None)
    assert set(verdicts) == set(REQUIRED_COLUMNS)
    assert all(v.state is ReuseState.INVALID for v in verdicts.values())
    assert all(v.must_recompute for v in verdicts.values())
    assert not all_outputs_reusable(verdicts)


def test_partial_metadata_response_yields_no_false_reuse() -> None:
    """Resposta transitória sem a chave `files` não pode virar reuse."""
    verdicts = _classify({"metadata": {"identifier": "ficha-2026-05"}})
    assert all(v.state is ReuseState.ABSENT for v in verdicts.values())
    assert not all_outputs_reusable(verdicts)


# --- presença sozinha nunca basta -------------------------------------------


def test_presence_alone_does_not_authorize_reuse() -> None:
    """`remote exists => skip` é exatamente o que este slice não pode fazer."""

    def probe(url: str) -> ParquetProbeResult:
        raise ValueError("footer ausente")

    verdicts = _classify(_full_metadata(size=10**9), probe=probe)
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

    verdicts = _classify(_full_metadata())
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
    verdicts = _classify(_full_metadata())
    lines = format_verdicts(verdicts).splitlines()
    assert len(lines) == len(REQUIRED_COLUMNS)
    assert all("reusable" in line for line in lines)


def test_classify_outputs_rejects_bad_month() -> None:
    with pytest.raises(ValueError, match="YYYY-MM"):
        classify_outputs("2026-5", metadata=_full_metadata(), probe=_probe_ok)


# --- lookups.json: presença + tamanho não bastam -----------------------------


def test_lookups_json_with_schema_keys_is_reusable() -> None:
    verdicts = _classify(_full_metadata())
    assert verdicts["lookups.json"].state is ReuseState.REUSABLE


def test_lookups_json_empty_object_is_invalid() -> None:
    """`{}` tem size > 0 e passaria por presença+tamanho."""
    verdicts = _classify(_full_metadata(), json_fetch=lambda url: {})
    assert verdicts["lookups.json"].state is ReuseState.INVALID
    assert "chaves ausentes" in verdicts["lookups.json"].detail


def test_lookups_json_missing_one_schema_key_is_invalid() -> None:
    def fetch(url: str) -> object:
        payload = {key: {} for key in LOOKUPS_JSON_REQUIRED_KEYS}
        del payload["municipios"]
        return payload

    verdicts = _classify(_full_metadata(), json_fetch=fetch)
    assert verdicts["lookups.json"].state is ReuseState.INVALID
    assert "municipios" in verdicts["lookups.json"].detail


def test_lookups_json_html_error_page_is_invalid() -> None:
    """Página de erro servida com 200 não pode virar materialização válida."""

    def fetch(url: str) -> object:
        raise ValueError("Expecting value: line 1 column 1 (char 0)")

    verdicts = _classify(_full_metadata(), json_fetch=fetch)
    assert verdicts["lookups.json"].state is ReuseState.INVALID
    assert "ilegível" in verdicts["lookups.json"].detail


def test_lookups_json_non_object_is_invalid() -> None:
    verdicts = _classify(_full_metadata(), json_fetch=lambda url: [1, 2, 3])
    assert verdicts["lookups.json"].state is ReuseState.INVALID
    assert "não é objeto JSON" in verdicts["lookups.json"].detail


def test_no_artifact_is_reusable_by_size_alone() -> None:
    """Nenhum artefato do contrato pode ser aprovado só por presença/tamanho."""

    def probe(url: str) -> ParquetProbeResult:
        raise ValueError("footer ausente")

    def fetch(url: str) -> object:
        raise ValueError("ilegível")

    verdicts = _classify(_full_metadata(size=10**9), probe=probe, json_fetch=fetch)
    assert all(v.state is ReuseState.INVALID for v in verdicts.values())


# --- metadata: retry só para transitório -------------------------------------


class _FakeClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url):
        self.calls += 1
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _response(status: int, payload=None) -> httpx.Response:
    request = httpx.Request("GET", "https://archive.org/metadata/ficha-2026-05")
    if payload is None:
        return httpx.Response(status, request=request, text="upstream indisponível")
    return httpx.Response(status, request=request, json=payload)


def test_metadata_retries_transient_timeout_then_succeeds() -> None:
    client = _FakeClient([httpx.ReadTimeout("timeout"), _response(200, {"files": []})])
    slept: list[float] = []

    result = fetch_item_metadata(MONTH, client=client, sleep=slept.append)

    assert result == {"files": []}
    assert client.calls == 2
    assert slept, "deveria ter esperado antes de repetir"


def test_metadata_retries_transient_status_then_succeeds() -> None:
    client = _FakeClient([_response(503), _response(200, {"files": []})])
    result = fetch_item_metadata(MONTH, client=client, sleep=lambda _: None)
    assert result == {"files": []}
    assert client.calls == 2


def test_metadata_gives_up_after_attempts_and_stays_ambiguous() -> None:
    client = _FakeClient([_response(503), _response(503), _response(503)])
    result = fetch_item_metadata(MONTH, client=client, attempts=3, sleep=lambda _: None)
    assert result is None
    assert client.calls == 3


def test_metadata_does_not_retry_structural_404() -> None:
    """404 é resposta estrutural: insistir só atrasaria a recomputação legítima."""
    client = _FakeClient([_response(404)])
    result = fetch_item_metadata(MONTH, client=client, sleep=lambda _: None)
    assert result is None
    assert client.calls == 1


def test_metadata_does_not_retry_invalid_json() -> None:
    request = httpx.Request("GET", "https://archive.org/metadata/ficha-2026-05")
    client = _FakeClient([httpx.Response(200, request=request, text="<html>nope</html>")])
    result = fetch_item_metadata(MONTH, client=client, sleep=lambda _: None)
    assert result is None
    assert client.calls == 1, "JSON inválido não é falha transitória"


def test_metadata_failure_never_becomes_reuse() -> None:
    client = _FakeClient([_response(503), _response(503), _response(503)])
    metadata = fetch_item_metadata(MONTH, client=client, attempts=3, sleep=lambda _: None)
    verdicts = _classify(metadata)
    assert not all_outputs_reusable(verdicts)
