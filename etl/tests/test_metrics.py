"""Testes de `ficha_etl.metrics` — Fase 0 da RFC 0001 (baseline real).

Cobertura:
  a) stage() mede wall-clock e RSS; to_json_dict tem chaves estáveis.
  b) sampler de disco: detecta crescimento; tolera arquivo deletado durante amostragem.
  c) MetricsRecorder: escreve metrics.json válido; falha de I/O vira warning, não exceção.
  d) integração leve: estágio envolvendo uma query DuckDB real produz métricas plausíveis.
"""

from __future__ import annotations

import collections
import json
import logging
import shutil
import time
from pathlib import Path

import duckdb
import pytest

from ficha_etl import metrics

# -----------------------------------------------------------------------------
# a) stage() — wall-clock, RSS, to_json_dict
# -----------------------------------------------------------------------------


def test_stage_measures_wall_clock_and_rss():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")

    with recorder.stage("noop") as handle:
        time.sleep(0.05)
        handle.rows_written = 42
        handle.bytes_written = 1000

    assert len(recorder.stages) == 1
    m = recorder.stages[0]
    assert m.wall_seconds > 0
    assert m.rss_peak_mib > 0
    assert m.rss_peak_delta_mib >= 0


def test_stage_to_json_dict_has_stable_keys():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("noop") as handle:
        handle.rows_written = 7
        handle.bytes_written = 123

    d = recorder.stages[0].to_json_dict()
    assert set(d.keys()) == {
        "stage",
        "wall_seconds",
        "rows_read",
        "rows_written",
        "bytes_read",
        "bytes_written",
        "files_read",
        "mb_per_second",
        "rows_per_second",
        "rss_peak_mib",
        "rss_peak_delta_mib",
        "duckdb_tmp_peak_mib",
        "workdir_peak_mib",
        "filesystem_used_peak_mib",
        "filesystem_total_mib",
        "filesystem_used_peak_percent",
        "casts_invalid",
        "quarantine_rows",
        "duplicate_rows",
        "started_at",
        "finished_at",
        "chunks",
        "extra",
    }
    assert d["stage"] == "noop"
    assert d["rows_written"] == 7
    assert d["bytes_written"] == 123


def test_stage_without_watch_dirs_has_none_disk_peaks():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("s"):
        pass
    m = recorder.stages[-1]
    assert m.duckdb_tmp_peak_mib is None
    assert m.workdir_peak_mib is None
    # Recorder criado sem filesystem_path -- nenhum stage amostra o mount.
    assert m.filesystem_used_peak_mib is None
    assert m.chunks is None


def test_stage_extra_field_serializes():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("s") as handle:
        handle.extra["chunks"] = 3
        handle.extra["fallback_encoding"] = "utf-8"
    d = recorder.stages[-1].to_json_dict()
    assert d["extra"] == {"chunks": 3, "fallback_encoding": "utf-8"}


def test_stage_records_metrics_even_if_body_raises():
    """Se o corpo do `with` levantar, o estágio ainda é registrado (parcial) —
    útil pra diagnosticar onde um estágio longo morreu."""
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with pytest.raises(ValueError, match="boom"):
        with recorder.stage("failing") as handle:
            handle.rows_read = 5
            raise ValueError("boom")

    assert len(recorder.stages) == 1
    assert recorder.stages[0].name == "failing"
    assert recorder.stages[0].rows_read == 5


def test_mb_per_second_and_rows_per_second_none_without_data():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("empty"):
        time.sleep(0.01)
    m = recorder.stages[-1]
    assert m.mb_per_second() is None
    assert m.rows_per_second() is None


def test_mb_per_second_and_rows_per_second_computed():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("full") as handle:
        time.sleep(0.05)
        handle.bytes_written = 10 * 1024 * 1024  # 10 MiB
        handle.rows_written = 100
    m = recorder.stages[-1]
    assert m.mb_per_second() is not None
    assert m.mb_per_second() > 0
    assert m.rows_per_second() is not None
    assert m.rows_per_second() > 0


# -----------------------------------------------------------------------------
# b) sampler de disco
# -----------------------------------------------------------------------------


def test_dir_size_bytes_missing_dir_returns_zero(tmp_path):
    missing = tmp_path / "nope"
    assert metrics._dir_size_bytes(missing) == 0


def test_dir_size_bytes_sums_files(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    (d / "a.txt").write_bytes(b"x" * 100)
    (d / "b.txt").write_bytes(b"y" * 50)
    assert metrics._dir_size_bytes(d) == 150


def test_dir_size_bytes_ignores_file_vanished_between_listing_and_stat(tmp_path, monkeypatch):
    """Simula a corrida real: os.walk já listou o arquivo, mas ele some antes do stat.

    Isso acontece de verdade no pipeline (partes temporárias de COPY sendo
    trocadas, spill do DuckDB liberado) — deve ser ignorado, não explodir o
    sampler inteiro por causa de um arquivo transitório.
    """
    d = tmp_path / "d"
    d.mkdir()
    (d / "a.txt").write_bytes(b"x" * 100)
    (d / "vanishing.txt").write_bytes(b"y" * 50)

    real_stat = Path.stat

    def flaky_stat(self, *args, **kwargs):
        if self.name == "vanishing.txt":
            raise FileNotFoundError(str(self))
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_stat)
    size = metrics._dir_size_bytes(d)
    assert size == 100  # só a.txt contou; vanishing.txt foi ignorado sem crash


def test_dir_size_bytes_ignores_any_oserror_not_just_file_not_found(tmp_path, monkeypatch):
    """Finding #2 do review adversarial: `_dir_size_bytes` deve engolir
    QUALQUER OSError (PermissionError, NotADirectoryError, etc.), não só
    FileNotFoundError."""
    d = tmp_path / "d"
    d.mkdir()
    (d / "a.txt").write_bytes(b"x" * 100)
    (d / "cursed.txt").write_bytes(b"y" * 50)

    real_stat = Path.stat

    def flaky_stat(self, *args, **kwargs):
        if self.name == "cursed.txt":
            raise PermissionError(str(self))
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_stat)
    assert metrics._dir_size_bytes(d) == 100


def test_disk_peak_sampler_thread_stops_and_warns_on_unexpected_error(
    tmp_path, monkeypatch, caplog
):
    """Finding #3: uma exceção genuinamente inesperada dentro do loop de
    amostragem não pode matar a thread daemon em silêncio -- tem que logar
    um warning e encerrar o loop de forma limpa (sem virar spam)."""
    d = tmp_path / "d"
    d.mkdir()
    sampler = metrics._DiskPeakSampler({"workdir": d}, interval=0.02)

    def boom(self):
        raise RuntimeError("erro inesperado simulado")

    monkeypatch.setattr(metrics._DiskPeakSampler, "_sample_once", boom)

    with caplog.at_level(logging.WARNING):
        sampler.start()
        assert sampler._thread is not None
        sampler._thread.join(timeout=1.0)

    assert not sampler._thread.is_alive()
    assert any("sampler" in r.message.lower() for r in caplog.records)


class _FakeStuckThread:
    """Dublê de threading.Thread que nunca reporta ter terminado -- simula
    uma thread de amostragem presa (ex.: os.walk pendurado num mount morto)."""

    def join(self, timeout: float | None = None) -> None:
        return None

    def is_alive(self) -> bool:
        return True


def test_disk_peak_sampler_stop_warns_if_thread_still_alive(tmp_path, caplog):
    """Finding #6: se a thread não morrer dentro do timeout do join, stop()
    deve avisar e seguir em frente sem bloquear -- nunca travar o teardown."""
    d = tmp_path / "d"
    d.mkdir()
    sampler = metrics._DiskPeakSampler({"workdir": d}, interval=0.01)
    sampler._thread = _FakeStuckThread()  # type: ignore[assignment]

    with caplog.at_level(logging.WARNING):
        peaks = sampler.stop()

    assert isinstance(peaks, dict)  # stop() seguiu em frente e devolveu algo
    assert any("thread" in r.message.lower() for r in caplog.records)


def test_disk_peak_sampler_detects_growth(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    sampler = metrics._DiskPeakSampler({"workdir": d}, interval=0.05)
    sampler.start()
    try:
        time.sleep(0.02)
        (d / "big.bin").write_bytes(b"0" * (2 * 1024 * 1024))
        time.sleep(0.2)
    finally:
        peaks = sampler.stop()
    assert peaks["workdir"] >= 2 * 1024 * 1024


def test_stage_disk_peak_tracks_workdir_growth(tmp_path):
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    workdir = tmp_path / "work"
    workdir.mkdir()

    with recorder.stage("writer", workdir=workdir, sample_interval=0.05):
        (workdir / "out.bin").write_bytes(b"x" * (3 * 1024 * 1024))
        time.sleep(0.2)

    m = recorder.stages[-1]
    assert m.workdir_peak_mib is not None
    assert m.workdir_peak_mib >= 2.9


# -----------------------------------------------------------------------------
# Finding F do review do owner na PR #70: picos de RSS/disco por chunk sem
# thread nova por chunk -- `current_peaks()` (leitura pontual, não pára o
# sampler) + `StageHandle.disk_peaks_snapshot()` (delega pro sampler do
# estágio corrente).
# -----------------------------------------------------------------------------


def test_disk_peak_sampler_current_peaks_does_not_stop_thread(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    sampler = metrics._DiskPeakSampler({"workdir": d}, interval=0.05)
    sampler.start()
    try:
        time.sleep(0.02)
        (d / "big.bin").write_bytes(b"0" * (2 * 1024 * 1024))
        time.sleep(0.15)
        peaks = sampler.current_peaks()
        assert peaks["workdir"] >= 2 * 1024 * 1024
        # A thread continua viva -- current_peaks() não é stop().
        assert sampler._thread is not None
        assert sampler._thread.is_alive()
    finally:
        sampler.stop()


def test_stage_handle_disk_peaks_snapshot_empty_without_sampler():
    handle = metrics.StageHandle(name="s")
    assert handle.disk_peaks_snapshot() == {}


def test_stage_handle_disk_peaks_snapshot_delegates_to_sampler(tmp_path):
    """MetricsRecorder.stage() preenche handle._sampler -- disk_peaks_snapshot()
    do handle tem que refletir o mesmo pico que o sampler observou, mesmo
    ANTES do estágio terminar (é justamente o ponto: espiar no meio)."""
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    workdir = tmp_path / "work"
    workdir.mkdir()

    with recorder.stage("writer", workdir=workdir, sample_interval=0.05) as handle:
        (workdir / "out.bin").write_bytes(b"x" * (3 * 1024 * 1024))
        time.sleep(0.15)
        mid_stage_peaks = handle.disk_peaks_snapshot()
        assert mid_stage_peaks.get("workdir", 0) >= 3 * 1024 * 1024

    # Depois do estágio terminar (sampler já parado), o snapshot continua
    # disponível (stop() preserva o último estado em _peaks).
    final_peaks = handle.disk_peaks_snapshot()
    assert final_peaks.get("workdir", 0) >= 3 * 1024 * 1024


# -----------------------------------------------------------------------------
# Finding #1 do review adversarial da PR #70: o sampler por nome de
# diretório (duckdb_tmp/workdir) não vê arquivos grandes que o pipeline cria
# FORA desses dirs (ex.: cache_dir/<month>/transform.duckdb, irmão de
# duckdb_tmp). `filesystem_path` mede o mount inteiro via
# `shutil.disk_usage`, que é a métrica que importa pro gate de 80% da RFC
# 0001 §19.
# -----------------------------------------------------------------------------


def test_disk_peak_sampler_filesystem_peak_sees_sibling_file_outside_watched_dirs(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    duckdb_tmp = cache_dir / "duckdb_tmp"
    duckdb_tmp.mkdir()

    baseline_used = shutil.disk_usage(cache_dir).used

    sampler = metrics._DiskPeakSampler(
        {"duckdb_tmp": duckdb_tmp}, interval=0.05, filesystem_path=cache_dir
    )
    sampler.start()
    try:
        time.sleep(0.02)
        # "transform.duckdb" simulado -- irmão de duckdb_tmp, NUNCA listado
        # em `_DiskPeakSampler._dirs` por nome (o cenário real do finding).
        sibling_db = cache_dir / "transform.duckdb"
        sibling_db.write_bytes(b"0" * (20 * 1024 * 1024))  # 20 MiB
        time.sleep(0.2)
    finally:
        peaks = sampler.stop()

    # O dir monitorado por nome continua vazio -- o arquivo não está dentro dele.
    assert peaks["duckdb_tmp"] == 0
    # Mas o pico de filesystem viu o crescimento, porque mede o mount
    # inteiro via disk_usage, não a soma dos dirs monitorados.
    assert peaks["filesystem"] > baseline_used


def test_disk_peak_sampler_filesystem_only_no_named_dirs(tmp_path):
    """Um sampler só com filesystem_path (sem duckdb_tmp_dir/workdir) ainda
    deve arrancar a thread e amostrar -- o guard `_has_anything_to_watch`
    não pode considerar isso "nada pra ver"."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    sampler = metrics._DiskPeakSampler({}, interval=0.05, filesystem_path=cache_dir)
    sampler.start()
    time.sleep(0.1)
    peaks = sampler.stop()
    assert "filesystem" in peaks
    assert peaks["filesystem"] > 0


def test_filesystem_usage_walks_up_to_existing_ancestor(tmp_path):
    """`path` pode ainda não existir quando o sampler arranca (ex.:
    cache_dir/<month>/ só é criado depois do 1º estágio) -- não deve
    devolver None nem explodir, só subir pro ancestral existente mais próximo."""
    missing = tmp_path / "does" / "not" / "exist" / "yet"
    usage = metrics._filesystem_usage(missing)
    assert usage is not None
    assert usage.used > 0
    assert usage.total > 0


def test_stage_filesystem_peak_recorded_for_every_stage_when_configured(tmp_path):
    """Diferente de duckdb_tmp/workdir (escolhidos por chamada de `stage()`),
    `filesystem_path` é configurado uma vez no `MetricsRecorder` e vale pra
    TODO estágio -- inclusive um sem nenhum watch dir próprio (ex.:
    "lookups", que hoje não passa duckdb_tmp_dir/workdir)."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    recorder = metrics.MetricsRecorder(
        month="2026-07", schema_version="1.0.0", filesystem_path=cache_dir
    )
    with recorder.stage("lookups", sample_interval=0.05):
        time.sleep(0.1)
    m = recorder.stages[-1]
    assert m.filesystem_used_peak_mib is not None
    assert m.filesystem_used_peak_mib > 0


# -----------------------------------------------------------------------------
# Finding A do review do owner na PR #70: o gate de 80% da RFC 0001 §19 é
# uma FRAÇÃO do mount ("abaixo de 80% da capacidade do runner"), não um
# valor absoluto de MiB -- o mesmo pico em MiB é folgado num runner grande e
# crítico num pequeno. StageMetrics precisa do denominador (`total`) e da
# fração já calculada (`percent`), não só do numerador.
# -----------------------------------------------------------------------------

_FakeDiskUsage = collections.namedtuple("_FakeDiskUsage", ["total", "used", "free"])


def test_filesystem_usage_returns_used_and_total_from_one_call(monkeypatch, tmp_path):
    calls: list[Path] = []

    def fake_disk_usage(path):
        calls.append(Path(path))
        return _FakeDiskUsage(total=1_000, used=400, free=600)

    monkeypatch.setattr(metrics.shutil, "disk_usage", fake_disk_usage)
    usage = metrics._filesystem_usage(tmp_path)

    assert usage is not None
    assert usage.used == 400
    assert usage.total == 1_000
    assert len(calls) == 1  # uma única chamada de disk_usage -- used e total vêm dela


def test_percent_helper_matches_manual_calc():
    assert metrics._percent(400, 1_000) == pytest.approx(40.0)
    assert metrics._percent(None, 1_000) is None
    assert metrics._percent(400, None) is None
    assert metrics._percent(400, 0) is None


def test_stage_filesystem_used_peak_percent_matches_manual_calculation(monkeypatch, tmp_path):
    """Monkeypatch shutil.disk_usage com (total, used) conhecidos e confere
    que filesystem_used_peak_percent bate com a conta manual used/total*100."""
    total_bytes = 1_000 * 1024 * 1024  # 1000 MiB
    used_bytes = 812 * 1024 * 1024  # 812 MiB

    def fake_disk_usage(path):
        return _FakeDiskUsage(total=total_bytes, used=used_bytes, free=total_bytes - used_bytes)

    monkeypatch.setattr(metrics.shutil, "disk_usage", fake_disk_usage)

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    recorder = metrics.MetricsRecorder(
        month="2026-07", schema_version="1.0.0", filesystem_path=cache_dir
    )
    with recorder.stage("s", sample_interval=0.05):
        time.sleep(0.1)

    m = recorder.stages[-1]
    assert m.filesystem_total_mib == pytest.approx(1000.0, rel=1e-6)
    assert m.filesystem_used_peak_mib == pytest.approx(812.0, rel=1e-6)
    expected_percent = used_bytes / total_bytes * 100
    assert m.filesystem_used_peak_percent == pytest.approx(expected_percent, rel=1e-6)

    d = m.to_json_dict()
    assert d["filesystem_total_mib"] == pytest.approx(1000.0, rel=1e-6)
    assert d["filesystem_used_peak_percent"] == pytest.approx(expected_percent, rel=1e-2)


def test_disk_peak_sampler_records_filesystem_total_alongside_used(monkeypatch, tmp_path):
    def fake_disk_usage(path):
        return _FakeDiskUsage(total=5_000, used=1_234, free=3_766)

    monkeypatch.setattr(metrics.shutil, "disk_usage", fake_disk_usage)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    sampler = metrics._DiskPeakSampler({}, interval=0.05, filesystem_path=cache_dir)
    sampler.start()
    time.sleep(0.1)
    peaks = sampler.stop()
    assert peaks["filesystem"] == 1_234
    assert peaks["filesystem_total"] == 5_000


# -----------------------------------------------------------------------------
# Finding #3 do review adversarial da PR #70: code_version precisa
# identificar o COMMIT que gerou o run, não a versão fixa do pacote.
# -----------------------------------------------------------------------------


def test_git_sha_uses_github_sha_env(monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "abc123deadbeef")
    assert metrics._git_sha() == "abc123deadbeef"


def test_git_sha_falls_back_to_git_subprocess(monkeypatch):
    monkeypatch.delenv("GITHUB_SHA", raising=False)

    class _FakeCompleted:
        def __init__(self, stdout, returncode=0):
            self.stdout = stdout
            self.returncode = returncode

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["git", "rev-parse"]:
            return _FakeCompleted("deadbeef1234\n")
        if cmd[:2] == ["git", "status"]:
            return _FakeCompleted("")  # working tree limpa
        raise AssertionError(f"unexpected git command {cmd}")

    monkeypatch.setattr(metrics.subprocess, "run", fake_run)
    assert metrics._git_sha() == "deadbeef1234"


def test_git_sha_marks_dirty_tree(monkeypatch):
    monkeypatch.delenv("GITHUB_SHA", raising=False)

    class _FakeCompleted:
        def __init__(self, stdout, returncode=0):
            self.stdout = stdout
            self.returncode = returncode

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["git", "rev-parse"]:
            return _FakeCompleted("deadbeef1234\n")
        if cmd[:2] == ["git", "status"]:
            return _FakeCompleted(" M some_file.py\n")
        raise AssertionError(f"unexpected git command {cmd}")

    monkeypatch.setattr(metrics.subprocess, "run", fake_run)
    assert metrics._git_sha() == "deadbeef1234-dirty"


def test_git_sha_returns_unknown_when_subprocess_fails(monkeypatch):
    monkeypatch.delenv("GITHUB_SHA", raising=False)

    def fake_run(cmd, **kwargs):
        raise OSError("git not installed (simulated)")

    monkeypatch.setattr(metrics.subprocess, "run", fake_run)
    assert metrics._git_sha() == "unknown"


def test_git_sha_returns_unknown_when_git_rev_parse_fails_cleanly(monkeypatch):
    """`git rev-parse` roda mas devolve returncode != 0 (ex.: não é um repo
    git) -- sem exceção nenhuma, só um resultado "falhou"."""
    monkeypatch.delenv("GITHUB_SHA", raising=False)

    class _FakeCompleted:
        def __init__(self, stdout, returncode):
            self.stdout = stdout
            self.returncode = returncode

    def fake_run(cmd, **kwargs):
        return _FakeCompleted("", 128)

    monkeypatch.setattr(metrics.subprocess, "run", fake_run)
    assert metrics._git_sha() == "unknown"


def test_envelope_uses_git_sha_as_code_version(monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "envelopesha123")
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    envelope = recorder.to_envelope()
    assert envelope["code_version"] == "envelopesha123"
    assert "package_version" in envelope


# -----------------------------------------------------------------------------
# Finding G do review do owner na PR #70: campos do §16 ainda ausentes --
# ibis_version no envelope; files_read/casts_invalid/quarantine_rows/
# duplicate_rows em StageMetrics (os três últimos sempre None na Fase 0,
# documentados como lacuna real, não inventados).
# -----------------------------------------------------------------------------


def test_envelope_includes_ibis_version():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    envelope = recorder.to_envelope()
    assert "ibis_version" in envelope
    assert envelope["ibis_version"] != ""


def test_ibis_version_returns_unknown_when_package_missing(monkeypatch):
    import importlib.metadata

    def fake_version(name):
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", fake_version)
    assert metrics._ibis_version() == "unknown"


def test_envelope_exposes_schema_version_unchanged():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="9.9.9")
    envelope = recorder.to_envelope()
    assert envelope["schema_version"] == "9.9.9"


def test_stage_files_read_is_settable_and_serializes():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("extract") as handle:
        handle.files_read = 37
    m = recorder.stages[-1]
    assert m.files_read == 37
    assert m.to_json_dict()["files_read"] == 37


def test_stage_files_read_defaults_to_none_when_not_applicable():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("cnpj_contatos"):
        pass
    m = recorder.stages[-1]
    assert m.files_read is None


def test_stage_casts_invalid_and_quarantine_rows_always_none_in_phase_0():
    """RFC 0001 §16 pede esses campos, mas o reader legado não tem esse
    conceito (ver docstring de StageMetrics) -- devem ficar None sempre,
    nunca um valor inventado."""
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("load_duckdb"):
        pass
    m = recorder.stages[-1]
    assert m.casts_invalid is None
    assert m.quarantine_rows is None
    d = m.to_json_dict()
    assert d["casts_invalid"] is None
    assert d["quarantine_rows"] is None


def test_stage_duplicate_rows_settable_via_handle():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("load_duckdb") as handle:
        handle.duplicate_rows = 3
    m = recorder.stages[-1]
    assert m.duplicate_rows == 3
    assert m.to_json_dict()["duplicate_rows"] == 3


# -----------------------------------------------------------------------------
# Finding #4 do review adversarial da PR #70: ChunkMetrics -- serialização
# estável (o teste de integração fim-a-fim com write_cnpjs_parquet_chunked
# vive em test_transform.py, junto do resto dos testes desse writer).
# -----------------------------------------------------------------------------


def test_chunk_metrics_to_json_dict():
    cm = metrics.ChunkMetrics(
        index=2, csv_name="Estabelecimentos2.csv", wall_seconds=1.2345, rows_written=10
    )
    d = cm.to_json_dict()
    assert d == {
        "index": 2,
        "csv_name": "Estabelecimentos2.csv",
        "wall_seconds": 1.234,
        "rows_written": 10,
        "bytes_read": None,
        "bytes_written": None,
        "status": "ok",
        "error": None,
        "rss_peak_mib": None,
        "duckdb_tmp_peak_mib": None,
        "workdir_peak_mib": None,
    }


def test_chunk_metrics_failed_status_serializes():
    cm = metrics.ChunkMetrics(
        index=1,
        csv_name="Estabelecimentos1.csv",
        wall_seconds=0.5,
        status="failed",
        error="RuntimeError: falha simulada",
    )
    d = cm.to_json_dict()
    assert d["status"] == "failed"
    assert d["error"] == "RuntimeError: falha simulada"
    assert d["rows_written"] is None


def test_stage_chunks_serialize_in_to_json_dict():
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("cnpjs_chunked") as handle:
        handle.chunks.append(
            metrics.ChunkMetrics(index=0, csv_name="a.csv", wall_seconds=0.5, rows_written=3)
        )
        handle.chunks.append(
            metrics.ChunkMetrics(index=1, csv_name="b.csv", wall_seconds=0.7, rows_written=5)
        )
    d = recorder.stages[-1].to_json_dict()
    assert len(d["chunks"]) == 2
    assert d["chunks"][0]["csv_name"] == "a.csv"
    assert d["chunks"][1]["rows_written"] == 5


# -----------------------------------------------------------------------------
# Finding #1 do review adversarial + regressão exigida (finding #4): uma
# falha no TEARDOWN do stage() (sampler.stop() -> _dir_size_bytes) nunca
# pode mascarar a exceção real do corpo do `with`, nem derrubar um corpo
# que teria terminado com sucesso.
# -----------------------------------------------------------------------------


def test_stage_teardown_failure_does_not_mask_body_exception(tmp_path, monkeypatch, caplog):
    """Corpo do `with` levanta sua própria exceção E o teardown (sampler
    ativo) também falha ao amostrar pela última vez -- a exceção que
    propaga tem que ser a ORIGINAL do corpo, não a do teardown, e um
    warning precisa ter sido logado."""
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    workdir = tmp_path / "work"
    workdir.mkdir()

    def boom_dir_size(path):
        raise PermissionError("simulated stale mount")

    monkeypatch.setattr(metrics, "_dir_size_bytes", boom_dir_size)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ValueError, match="original body error") as exc_info:
            with recorder.stage("failing_with_disk", workdir=workdir) as handle:
                handle.rows_read = 1
                raise ValueError("original body error")

    # A exceção propagada é a original -- não foi substituída pela do
    # teardown (que, se tivesse vazado, apareceria como PermissionError).
    assert exc_info.type is ValueError
    assert any("metrics" in r.message.lower() for r in caplog.records)


def test_stage_teardown_failure_alone_does_not_raise(tmp_path, monkeypatch, caplog):
    """Corpo do `with` termina com sucesso, só o teardown falha -- nenhuma
    exceção pode propagar (o with tem que se comportar como bem-sucedido
    do ponto de vista do chamador), mas um warning precisa aparecer."""
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    workdir = tmp_path / "work"
    workdir.mkdir()

    def boom_dir_size(path):
        raise PermissionError("simulated stale mount")

    monkeypatch.setattr(metrics, "_dir_size_bytes", boom_dir_size)

    with caplog.at_level(logging.WARNING):
        with recorder.stage("ok_body_bad_teardown", workdir=workdir) as handle:
            handle.rows_written = 5
        # Nenhuma exceção chegou até aqui -- se chegasse, o teste já teria
        # falhado no próprio `with` acima.

    assert any("metrics" in r.message.lower() for r in caplog.records)


# -----------------------------------------------------------------------------
# c) MetricsRecorder — metrics.json
# -----------------------------------------------------------------------------


def test_recorder_writes_valid_json(tmp_path):
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with recorder.stage("s1") as handle:
        handle.rows_written = 10

    out = tmp_path / "metrics" / "transform_metrics.json"
    recorder.write_json(out)

    assert out.exists()
    data = json.loads(out.read_text())
    assert data["month"] == "2026-07"
    assert data["schema_version"] == "1.0.0"
    assert "code_version" in data
    assert "duckdb_version" in data
    assert "pragmas" in data
    assert isinstance(data["stages"], list)
    assert data["stages"][0]["stage"] == "s1"
    assert data["stages"][0]["rows_written"] == 10


def test_recorder_write_json_failure_is_warning_not_exception(tmp_path, monkeypatch, caplog):
    """Falha de I/O ao escrever metrics.json não pode propagar (RFC 0001 §16:
    métricas nunca derrubam o pipeline). Monkeypatch em vez de chmod porque
    testes podem rodar como root, que ignora permissões de diretório.
    """
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    out = tmp_path / "metrics" / "transform_metrics.json"

    def boom(self, *args, **kwargs):
        raise OSError("disk full (simulated)")

    monkeypatch.setattr(Path, "write_text", boom)

    with caplog.at_level(logging.WARNING):
        recorder.write_json(out)  # não deve levantar

    assert any("metrics" in r.message.lower() for r in caplog.records)


def test_capture_pragmas_reads_effective_settings():
    con = duckdb.connect()
    try:
        con.execute("PRAGMA memory_limit='512MB'")
        con.execute("PRAGMA threads=2")
        recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
        recorder.capture_pragmas(con)
        assert recorder.pragmas["threads"] == "2"
        assert "memory_limit" in recorder.pragmas
    finally:
        con.close()


def test_capture_pragmas_failure_is_warning_not_exception(caplog):
    """Conexão já fechada -> duckdb.Error ao consultar current_setting; deve virar warning."""
    con = duckdb.connect()
    con.close()
    recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
    with caplog.at_level(logging.WARNING):
        recorder.capture_pragmas(con)  # não deve levantar
    assert recorder.pragmas == {}


# -----------------------------------------------------------------------------
# d) Integração leve com DuckDB real
# -----------------------------------------------------------------------------


def test_stage_integration_with_duckdb_query():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE t AS SELECT * FROM range(1000) AS r(i)")
        recorder = metrics.MetricsRecorder(month="2026-07", schema_version="1.0.0")
        with recorder.stage("count_query") as handle:
            n = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            handle.rows_written = n
        m = recorder.stages[-1]
        assert m.rows_written == 1000
        assert m.wall_seconds >= 0
        assert m.rss_peak_mib > 0
        assert m.to_json_dict()["rows_written"] == 1000
    finally:
        con.close()
