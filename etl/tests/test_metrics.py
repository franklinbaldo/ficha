"""Testes de `ficha_etl.metrics` — Fase 0 da RFC 0001 (baseline real).

Cobertura:
  a) stage() mede wall-clock e RSS; to_json_dict tem chaves estáveis.
  b) sampler de disco: detecta crescimento; tolera arquivo deletado durante amostragem.
  c) MetricsRecorder: escreve metrics.json válido; falha de I/O vira warning, não exceção.
  d) integração leve: estágio envolvendo uma query DuckDB real produz métricas plausíveis.
"""

from __future__ import annotations

import json
import logging
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
        "mb_per_second",
        "rows_per_second",
        "rss_peak_mib",
        "rss_peak_delta_mib",
        "duckdb_tmp_peak_mib",
        "workdir_peak_mib",
        "started_at",
        "finished_at",
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
