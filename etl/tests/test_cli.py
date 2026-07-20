"""Testes de `ficha_etl.cli` -- entrypoint `ficha-etl ...` (cyclopts + rich).

Cobertura:
  a) roteamento de subcomandos + flags (--month, --skip-upload, --no-verify,
     ...) chegam corretas nas funções _cmd_* -- não testa efeitos de rede.
  b) validação de mês inválido -> exit 2 em todo subcomando que a faz.
  c) contrato stdout/stderr de list-snapshots/list-files -- .github/workflows/
     etl-bootstrap.yml e etl-smoke.yml fazem
     `SNAPSHOTS=$(uv run ficha-etl list-snapshots)` e grepam por linhas
     YYYY-MM puras; qualquer coisa além da lista de meses em stdout quebra
     esses workflows em produção.
  d) argumento obrigatório faltando -> SystemExit não-zero (cyclopts, não
     mais argparse, mas o contrato de "falha alto e claro" é o mesmo).
"""

from __future__ import annotations

import pytest

from ficha_etl import cli, smoke, upstream


def _capture(capsys):
    out = capsys.readouterr()
    return out.out, out.err


# -----------------------------------------------------------------------------
# a)/b) roteamento + validação de mês
# -----------------------------------------------------------------------------


def test_download_rejects_invalid_month(capsys):
    code = cli.main(["download", "--month", "not-a-month", "--target", "x"])
    assert code == 2
    out, err = _capture(capsys)
    assert "month must be YYYY-MM" in err
    assert out == ""


def test_list_files_rejects_invalid_month(capsys):
    code = cli.main(["list-files", "--month", "2026-13"])
    assert code == 2


def test_transform_rejects_invalid_month(capsys):
    code = cli.main(["transform", "--month", "bad", "--output", "out"])
    assert code == 2


def test_fetch_rejects_invalid_month(capsys):
    code = cli.main(["fetch", "--month", "bad", "--file", "Empresas0.zip"])
    assert code == 2


def test_pack_rejects_invalid_month(capsys):
    code = cli.main(["pack", "--month", "bad"])
    assert code == 2


def test_run_rejects_invalid_month(capsys):
    code = cli.main(["run", "--month", "bad"])
    assert code == 2


def test_run_flags_reach_cmd_run(monkeypatch, capsys, tmp_path):
    captured = {}

    def fake_cmd_run(
        month, *, cache_dir, output_dir, manifest_path, skip_upload, verify, verify_sample_size
    ):
        captured.update(
            month=month,
            cache_dir=cache_dir,
            output_dir=output_dir,
            manifest_path=manifest_path,
            skip_upload=skip_upload,
            verify=verify,
            verify_sample_size=verify_sample_size,
        )
        return 0

    monkeypatch.setattr(cli, "_cmd_run", fake_cmd_run)
    code = cli.main(
        [
            "run",
            "--month",
            "2026-01",
            "--skip-upload",
            "--no-verify",
            "--cache-dir",
            str(tmp_path),
        ]
    )
    assert code == 0
    assert captured["month"] == "2026-01"
    assert captured["skip_upload"] is True
    assert captured["verify"] is False
    assert captured["cache_dir"] == tmp_path


def test_transform_verify_defaults_false_unlike_run(monkeypatch):
    """`run --verify` defaults True; `transform --verify` defaults False --
    same flag name, different default, both preserved from the pre-cyclopts
    argparse setup (two separate add_argument calls with different
    defaults, now two separate cyclopts commands with different defaults
    on the same parameter name)."""
    captured = {}

    def fake_cmd_transform(month, output, cache_dir, strict, verify, verify_sample_size):
        captured["verify"] = verify
        return 0

    monkeypatch.setattr(cli, "_cmd_transform", fake_cmd_transform)
    code = cli.main(["transform", "--month", "2026-01", "--output", "out"])
    assert code == 0
    assert captured["verify"] is False


def test_smoke_routes_to_cmd_smoke(monkeypatch):
    monkeypatch.setattr(cli, "_cmd_smoke", lambda: 0)
    assert cli.main(["smoke"]) == 0


def test_list_snapshots_routes_to_cmd_list_snapshots(monkeypatch):
    monkeypatch.setattr(cli, "_cmd_list_snapshots", lambda: 0)
    assert cli.main(["list-snapshots"]) == 0


# -----------------------------------------------------------------------------
# c) contrato stdout/stderr -- list-snapshots/list-files
# -----------------------------------------------------------------------------


def test_list_snapshots_stdout_is_pure_month_lines(monkeypatch, capsys):
    """.github/workflows/etl-bootstrap.yml: MONTH=$(echo "$OUTPUT" | grep -E
    '^[0-9]{4}-(0[1-9]|1[0-2])$' | sort | tail -n 1) -- stdout must be
    exactly one YYYY-MM per line, nothing else. The summary count goes to
    stderr specifically so it can't pollute that capture.
    """
    monkeypatch.setattr(upstream, "discover_token", lambda: "tok")
    monkeypatch.setattr(upstream, "list_snapshots", lambda token: ["2026-01", "2026-02"])

    code = cli.main(["list-snapshots"])

    assert code == 0
    out, err = _capture(capsys)
    lines = out.splitlines()
    assert lines == ["2026-01", "2026-02"]
    assert "2 snapshots" in err


def test_list_snapshots_empty_result_is_an_error(monkeypatch, capsys):
    """Upstream RFB always has 35+ months since 2023-05; an empty list is a
    real failure (layout change, truncated response), not "no data yet" --
    exit 0 here previously masked two monthly cron failures.
    """
    monkeypatch.setattr(upstream, "discover_token", lambda: "tok")
    monkeypatch.setattr(upstream, "list_snapshots", lambda token: [])

    code = cli.main(["list-snapshots"])

    assert code == 1
    out, err = _capture(capsys)
    assert out == ""
    assert "no YYYY-MM folders found" in err


def test_list_files_stdout_has_no_month_summary_line(monkeypatch, capsys):
    monkeypatch.setattr(upstream, "discover_token", lambda: "tok")
    monkeypatch.setattr(
        upstream,
        "list_files",
        lambda token, month: [
            upstream.FileEntry(name="Empresas0.zip", size=100, etag="e1", content_type="zip"),
            upstream.FileEntry(name="Socios0.zip", size=200, etag="e2", content_type="zip"),
        ],
    )

    code = cli.main(["list-files", "--month", "2026-01"])

    assert code == 0
    out, err = _capture(capsys)
    assert "Empresas0.zip" in out
    assert "Socios0.zip" in out
    assert "files, " in err
    assert "files, " not in out


def test_smoke_all_ok_prints_ok_to_stdout(monkeypatch, capsys):
    report = smoke.SmokeReport(
        upstream_ok=True, upstream_detail="200", mirror_ok=True, mirror_detail="200"
    )
    monkeypatch.setattr(smoke, "run_smoke", lambda: report)

    code = cli.main(["smoke"])

    assert code == 0
    out, _err = _capture(capsys)
    assert "acessíveis" in out


def test_smoke_mirror_down_is_blocking_and_exits_1(monkeypatch, capsys):
    report = smoke.SmokeReport(
        upstream_ok=True, upstream_detail="200", mirror_ok=False, mirror_detail="503"
    )
    monkeypatch.setattr(smoke, "run_smoke", lambda: report)

    code = cli.main(["smoke"])

    assert code == 1
    _out, err = _capture(capsys)
    assert "bloqueante" in err


# -----------------------------------------------------------------------------
# d) argumento obrigatório faltando
# -----------------------------------------------------------------------------


def test_missing_required_argument_exits_nonzero():
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["run"])
    assert exc_info.value.code != 0


def test_unknown_command_exits_nonzero():
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["not-a-real-command"])
    assert exc_info.value.code != 0
