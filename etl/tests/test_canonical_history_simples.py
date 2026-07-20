"""Historical canonical simples dataset orchestration tests (#97 slice 4)."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from ficha_etl import canonical_history_simples, registry


def _row(**overrides: str) -> dict[str, str]:
    row = dict.fromkeys(registry.SIMPLES_COLUMNS, "")
    row.update(
        cnpj_basico="00000001",
        opcao_simples="S",
        data_opcao_simples="20200115",
        data_exclusao_simples="",
        opcao_mei="N",
        data_opcao_mei="",
        data_exclusao_mei="",
    )
    row.update(overrides)
    return row


def _zip_bytes(rows: list[dict[str, str]], *, extra_file: bool = False) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(
        buffer, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n"
    )
    writer.writerows([[row[name] for name in registry.SIMPLES_COLUMNS] for row in rows])
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("K3241.K03200Y0.D60719.SIMPLES.CSV", buffer.getvalue().encode("latin-1"))
        if extra_file:
            archive.writestr("unexpected.txt", b"extra")
    return output.getvalue()


def _write_zip(path: Path, rows: list[dict[str, str]], *, extra_file: bool = False) -> None:
    path.write_bytes(_zip_bytes(rows, extra_file=extra_file))


def test_simples_remote_uses_historical_ia_raw_path():
    remote = canonical_history_simples.simples_remote("2026-04")
    assert remote.name == "Simples.zip"
    assert remote.kind == "simples"
    assert remote.url == "https://archive.org/download/ficha-2026-04/raw/Simples.zip"

    with pytest.raises(ValueError, match="YYYY-MM"):
        canonical_history_simples.simples_remote("April-2026")


def test_preflight_failure_reports_without_downloading(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "HEAD"
        return httpx.Response(503)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="preflight failed"):
            canonical_history_simples.run_historical_simples(
                "2026-04", tmp_path / "run", client=client
            )
    assert not (tmp_path / "run").exists()


def test_run_offline_complete_writes_durable_manifest(tmp_path):
    """One offline ZIP with an exact duplicate, a genuinely conflicting
    duplicate, and a malformed date -- exercises the full download
    (override)/extract/checksum/write/manifest path with no network at
    all.
    """
    zip_path = tmp_path / "fixture.zip"
    _write_zip(
        zip_path,
        [
            _row(cnpj_basico="00000001"),
            _row(cnpj_basico="00000001"),  # exact duplicate
            _row(cnpj_basico="00000002", opcao_simples="CONFLICT_A"),
            _row(cnpj_basico="00000002", opcao_simples="CONFLICT_B"),  # conflicting duplicate
            _row(cnpj_basico="00000003", data_opcao_simples="not-a-date"),  # malformed date
        ],
    )
    root = tmp_path / "run"

    result = canonical_history_simples.run_historical_simples(
        "2026-04", root, sample_size=10, zip_override=zip_path
    )

    manifest = result.manifest
    assert manifest["status"] == "ok"
    assert manifest["table"] == "simples"
    assert manifest["month"] == "2026-04"
    assert manifest["source"]["name"] == "Simples.zip"
    assert manifest["source"]["acquisition"] == "local-override"
    assert len(manifest["source"]["zip"]["sha256"]) == 64
    assert len(manifest["source"]["csv"]["sha256"]) == 64
    assert len(manifest["output"]["sha256"]) == 64
    assert len(manifest["quality"]["sha256"]) == 64
    assert len(manifest["metrics"]["sha256"]) == 64
    assert manifest["duckdb_version"]
    assert manifest["source_commit"]
    assert set(manifest["code"]) == {
        "canonical_history_simples",
        "canonical_history",
        "canonical_shadow",
        "transform",
        "registry",
        "sources",
    }

    summary = manifest["quality_summary"]
    assert summary["rows_raw"] == 5
    assert summary["rows_canonical"] == 3  # two duplicate pairs each collapse to 1
    assert summary["duplicate_key_count"] == 2  # two distinct keys are duplicated
    assert summary["duplicate_key_rows"] == 2  # one excess row each
    assert summary["conflicting_key_count"] == 1
    assert summary["conflicting_sample"] == [{"cnpj_basico": "00000002"}]
    assert summary["invalid_casts_by_column"]["data_opcao_simples"] == 1
    assert summary["schema_matches"] is True
    assert summary["sample_mismatches"] == 0

    resource = manifest["resource_summary"]
    assert resource["scope"] == "canonical-writer-stage"
    assert isinstance(resource["wall_seconds"], (int, float))
    assert resource["wall_seconds"] >= 0

    # Explicit primary-key ordering and per-row lineage.
    import duckdb

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT cnpj_basico, opcao_simples, _source_file FROM read_parquet(?)",
            [str(result.output_path)],
        ).fetchall()
    finally:
        con.close()
    assert [row[0] for row in rows] == ["00000001", "00000002", "00000003"]
    assert rows[1] == ("00000002", "CONFLICT_A", "Simples.zip")  # deterministic survivor
    assert all(row[2] == "Simples.zip" for row in rows)

    # Disk lifecycle: ZIP deleted after extraction, extracted/work cleaned
    # up after success -- only canonical output and evidence remain.
    assert not any((root / "raw").glob("*.zip"))
    assert not (root / "extracted").exists()
    assert not (root / "work").exists()
    assert result.output_path.exists()


def test_run_downloads_via_mock_transport(tmp_path):
    payload = _zip_bytes([_row()])
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, str(request.url)))
        if request.method == "HEAD":
            return httpx.Response(200, headers={"content-length": str(len(payload))})
        return httpx.Response(200, content=payload, headers={"content-length": str(len(payload))})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = canonical_history_simples.run_historical_simples(
            "2026-04", tmp_path / "run", sample_size=1, client=client
        )

    assert result.manifest["status"] == "ok"
    assert result.manifest["source"]["acquisition"] == "downloaded"
    heads = [url for method, url in requests_seen if method == "HEAD"]
    gets = [url for method, url in requests_seen if method == "GET"]
    assert len(heads) == 1
    assert len(gets) == 1


def test_run_rejects_invalid_override_path_before_any_network(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(
            f"network was touched ({request.method} {request.url}) despite an invalid "
            "override that should fail before any I/O"
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FileNotFoundError, match="Simples.zip"):
            canonical_history_simples.run_historical_simples(
                "2026-04",
                tmp_path / "run",
                zip_override=tmp_path / "does-not-exist.zip",
                client=client,
            )
    assert not (tmp_path / "run").exists()


def test_run_rejects_override_that_is_not_a_valid_zip(tmp_path):
    not_a_zip = tmp_path / "fake.zip"
    not_a_zip.write_bytes(b"not actually a zip file")

    with pytest.raises(RuntimeError, match="not a valid ZIP"):
        canonical_history_simples.run_historical_simples(
            "2026-04", tmp_path / "run", zip_override=not_a_zip
        )
    assert not (tmp_path / "run").exists()


def test_multiple_zip_members_fail_with_durable_failure_evidence(tmp_path):
    zip_path = tmp_path / "fixture.zip"
    _write_zip(zip_path, [_row()], extra_file=True)
    root = tmp_path / "run"

    with pytest.raises(RuntimeError, match="expected exactly one extracted CSV"):
        canonical_history_simples.run_historical_simples("2026-04", root, zip_override=zip_path)

    failure = json.loads((root / "evidence" / "simples.history.failure.json").read_text())
    assert failure["status"] == "failed"
    assert "expected exactly one" in failure["error"]
    assert failure["source"]["name"] == "Simples.zip"
    assert len(failure["source"]["zip"]["sha256"]) == 64  # zip was checksummed before failing
    # The downloaded (copied-from-override) ZIP must not linger on disk just
    # because its own extraction failed.
    assert not any((root / "raw").glob("*.zip"))


def test_cli_rejects_malformed_zip_override(tmp_path):
    result = canonical_history_simples.main(
        ["--month", "2026-04", "--root", str(tmp_path / "run"), "--zip", "not-a-valid-entry"]
    )
    assert result == 2


def test_cli_rejects_override_name_that_isnt_simples_zip(tmp_path):
    zip_path = tmp_path / "fixture.zip"
    _write_zip(zip_path, [_row()])

    result = canonical_history_simples.main(
        [
            "--month",
            "2026-04",
            "--root",
            str(tmp_path / "run"),
            "--zip",
            f"Empresas0.zip={zip_path}",
        ]
    )
    assert result == 1
    assert not (tmp_path / "run").exists()


def test_cli_offline_end_to_end(tmp_path, capsys):
    zip_path = tmp_path / "fixture.zip"
    _write_zip(zip_path, [_row(), _row()])  # exact dup, collapses
    root = tmp_path / "run"

    result = canonical_history_simples.main(
        [
            "--month",
            "2026-04",
            "--root",
            str(root),
            "--sample-size",
            "5",
            "--zip",
            f"Simples.zip={zip_path}",
        ]
    )

    assert result == 0
    out = capsys.readouterr().out
    assert "historical canonical simples dataset written" in out
    assert (root / "evidence" / "simples.history.json").exists()
