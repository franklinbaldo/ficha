"""Historical canonical empresa dataset orchestration tests (#97 slice 3)."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from ficha_etl import canonical_history_empresa, registry

_ALL_PARTS = [f"Empresas{n}.zip" for n in range(10)]


def _row(**overrides: str) -> dict[str, str]:
    row = dict.fromkeys(registry.EMPRESA_COLUMNS, "")
    row.update(
        cnpj_basico="00000001",
        razao_social="ACME LTDA",
        natureza_juridica="2062",
        qualificacao_responsavel="49",
        capital_social="150000,00",
        porte_empresa="03",
    )
    row.update(overrides)
    return row


def _zip_bytes(rows: list[dict[str, str]], *, extra_file: bool = False) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(
        buffer, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n"
    )
    writer.writerows([[row[name] for name in registry.EMPRESA_COLUMNS] for row in rows])
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("K3241.K03200Y0.D60719.EMPRECSV", buffer.getvalue().encode("latin-1"))
        if extra_file:
            archive.writestr("unexpected.txt", b"extra")
    return output.getvalue()


def _write_offline_dataset(
    root: Path, rows_by_part: dict[str, list[dict[str, str]]]
) -> dict[str, Path]:
    """Ten tiny synthetic ZIPs, one per expected empresa part name -- empty
    unless `rows_by_part` supplies rows for it. Returns the zip_overrides
    dict ready to pass to run_historical_empresa_dataset."""
    root.mkdir(parents=True, exist_ok=True)
    overrides: dict[str, Path] = {}
    for name in _ALL_PARTS:
        path = root / name
        path.write_bytes(_zip_bytes(rows_by_part.get(name, [_row(cnpj_basico=name[8])])))
        overrides[name] = path
    return overrides


def test_empresa_remotes_lists_complete_ten_part_set():
    remotes = canonical_history_empresa.empresa_remotes("2026-04")
    assert [r.name for r in remotes] == _ALL_PARTS
    assert all(r.kind == "empresas" for r in remotes)
    assert remotes[3].url == "https://archive.org/download/ficha-2026-04/raw/Empresas3.zip"

    with pytest.raises(ValueError, match="YYYY-MM"):
        canonical_history_empresa.empresa_remotes("April-2026")


def test_preflight_reports_missing_parts_without_downloading():
    remotes = canonical_history_empresa.empresa_remotes("2026-04")
    methods_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods_seen.append(request.method)
        # Empresas7.zip and Empresas9.zip are "missing" from the mirror.
        if request.url.path.endswith(("Empresas7.zip", "Empresas9.zip")):
            return httpx.Response(404)
        return httpx.Response(200, headers={"content-length": "123"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        missing = canonical_history_empresa.preflight_remote_availability(remotes, client=client)

    assert missing == ["Empresas7.zip", "Empresas9.zip"]
    assert methods_seen == ["HEAD"] * 10  # preflight never downloads (GET)


def test_run_refuses_partial_source_set_before_downloading_anything(tmp_path):
    """A run with some parts overridden and the rest failing preflight must
    fail closed BEFORE any download starts -- never process fewer than ten
    real parts."""
    zip_path = tmp_path / "Empresas0.zip"
    zip_path.write_bytes(_zip_bytes([_row()]))
    overrides = {"Empresas0.zip": zip_path}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)  # every non-overridden remote "doesn't exist"

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="preflight failed"):
            canonical_history_empresa.run_historical_empresa_dataset(
                "2026-04",
                tmp_path / "run",
                zip_overrides=overrides,
                client=client,
            )

    assert not (tmp_path / "run").exists() or not any((tmp_path / "run" / "raw").glob("*.zip"))


def test_run_offline_complete_dataset_writes_durable_manifest(tmp_path):
    """Ten tiny offline ZIPs with a deliberate cross-part duplicate: the
    same cnpj_basico appears in Empresas0.zip and Empresas6.zip with
    different payloads (a genuine conflict), collapsing to one canonical
    row. Exercises the full download(override)/extract/checksum/write/
    manifest path with no network access at all.
    """
    root = tmp_path / "dataset"
    rows_by_part = {name: [_row(cnpj_basico=f"{i:08d}")] for i, name in enumerate(_ALL_PARTS)}
    rows_by_part["Empresas0.zip"].append(_row(cnpj_basico="00000099", razao_social="CONFLICT A"))
    rows_by_part["Empresas6.zip"].append(_row(cnpj_basico="00000099", razao_social="CONFLICT B"))
    overrides = _write_offline_dataset(root / "fixtures", rows_by_part)

    result = canonical_history_empresa.run_historical_empresa_dataset(
        "2026-04",
        root / "run",
        sample_size=10,
        zip_overrides=overrides,
    )

    manifest = result.manifest
    assert manifest["status"] == "ok"
    assert manifest["table"] == "empresa"
    assert manifest["month"] == "2026-04"
    assert [entry["name"] for entry in manifest["sources"]] == _ALL_PARTS
    assert all(entry["acquisition"] == "local-override" for entry in manifest["sources"])
    assert all(len(entry["zip"]["sha256"]) == 64 for entry in manifest["sources"])
    assert all(len(entry["csv"]["sha256"]) == 64 for entry in manifest["sources"])
    assert len(manifest["output"]["sha256"]) == 64
    assert len(manifest["quality"]["sha256"]) == 64
    assert len(manifest["metrics"]["sha256"]) == 64
    assert manifest["duckdb_version"]
    assert manifest["source_commit"]
    assert set(manifest["code"]) == {
        "canonical_history_empresa",
        "canonical_shadow",
        "transform",
        "registry",
        "sources",
    }

    summary = manifest["quality_summary"]
    assert summary["rows_raw"] == 12  # 10 base rows + 2 extra conflicting rows (0 and 6)
    assert summary["rows_canonical"] == 11  # the conflicting pair collapses to 1
    assert summary["duplicate_key_count"] == 1
    assert summary["duplicate_key_rows"] == 1
    assert summary["conflicting_key_count"] == 1
    assert summary["conflicting_sample"][0]["cnpj_basico"] == "00000099"
    assert sorted(summary["conflicting_sample"][0]["source_files"]) == [
        "Empresas0.zip",
        "Empresas6.zip",
    ]
    assert summary["schema_matches"] is True
    assert summary["sample_mismatches"] == 0

    resource = manifest["resource_summary"]
    assert resource["files_read"] == 10  # not the single-part runner's hardcoded 1
    assert resource["scope"] == "canonical-writer-stage"  # not total orchestration time
    # Pinned against metrics.StageMetrics.to_json_dict()'s real, STABLE key
    # names (wall_seconds/rss_peak_delta_mib) -- a mismatched key here would
    # silently produce an all-null resource_summary instead of failing.
    assert isinstance(resource["wall_seconds"], (int, float))
    assert resource["wall_seconds"] >= 0

    # Per-row _source_file lineage: every surviving row's lineage must be a
    # real part name, and the conflict survivor's lineage must be one of
    # the two contributing parts (not an unrelated constant).
    import duckdb

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT cnpj_basico, _source_file FROM read_parquet(?) ORDER BY cnpj_basico",
            [str(result.output_path)],
        ).fetchall()
    finally:
        con.close()
    assert len(rows) == 11
    lineage_by_key = dict(rows)
    assert lineage_by_key["00000099"] in ("Empresas0.zip", "Empresas6.zip")
    assert lineage_by_key["00000000"] == "Empresas0.zip"
    assert lineage_by_key["00000005"] == "Empresas5.zip"

    # Disk lifecycle: ZIPs deleted after extraction, extracted/work cleaned
    # up after success -- only the canonical output and evidence remain.
    assert not any((root / "run" / "raw").glob("*.zip"))
    assert not (root / "run" / "extracted").exists()
    assert not (root / "run" / "work").exists()
    assert result.output_path.exists()


def test_run_downloads_via_mock_transport_with_no_overrides(tmp_path):
    """Full network path (mocked) for all ten parts -- no local overrides at
    all, proving download + preflight + extraction works end to end, not
    just the override shortcut."""
    payload = _zip_bytes([_row()])
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, str(request.url)))
        if request.method == "HEAD":
            return httpx.Response(200, headers={"content-length": str(len(payload))})
        return httpx.Response(200, content=payload, headers={"content-length": str(len(payload))})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = canonical_history_empresa.run_historical_empresa_dataset(
            "2026-04",
            tmp_path / "run",
            sample_size=1,
            client=client,
        )

    assert result.manifest["status"] == "ok"
    assert all(entry["acquisition"] == "downloaded" for entry in result.manifest["sources"])
    heads = [url for method, url in requests_seen if method == "HEAD"]
    gets = [url for method, url in requests_seen if method == "GET"]
    assert len(heads) == 10  # preflight HEADs every part first
    assert len(gets) == 10  # then downloads every part


def test_missing_expected_part_in_source_set_fails_closed_via_sources_py(tmp_path):
    """sources.canonical_inventory() is the single source of truth for the
    expected part set -- empresa_remotes() must always return exactly ten,
    which run_historical_empresa_dataset then requires completely (covered
    by write_canonical_dataset's own guard, exercised indirectly here via a
    corrupted zip_overrides dict that omits one real part and is not
    preflight-reachable)."""
    root = tmp_path / "dataset"
    rows_by_part = {name: [_row(cnpj_basico=f"{i:08d}")] for i, name in enumerate(_ALL_PARTS)}
    overrides = _write_offline_dataset(root / "fixtures", rows_by_part)
    del overrides["Empresas4.zip"]  # simulate a missing physical part

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="preflight failed"):
            canonical_history_empresa.run_historical_empresa_dataset(
                "2026-04",
                root / "run",
                zip_overrides=overrides,
                client=client,
            )


def test_multiple_zip_members_fail_with_durable_failure_evidence(tmp_path):
    root = tmp_path / "dataset"
    rows_by_part = {name: [_row(cnpj_basico=f"{i:08d}")] for i, name in enumerate(_ALL_PARTS)}
    overrides = _write_offline_dataset(root / "fixtures", rows_by_part)
    # Corrupt one override to contain two members.
    bad_zip = root / "fixtures" / "bad.zip"
    bad_zip.write_bytes(_zip_bytes([_row()], extra_file=True))
    overrides["Empresas3.zip"] = bad_zip

    with pytest.raises(RuntimeError, match="expected exactly one extracted CSV"):
        canonical_history_empresa.run_historical_empresa_dataset(
            "2026-04",
            root / "run",
            zip_overrides=overrides,
        )

    failure = json.loads((root / "run" / "evidence" / "empresa.history.failure.json").read_text())
    assert failure["status"] == "failed"
    assert "expected exactly one" in failure["error"]
    # Sources processed before the failure are still recorded.
    assert any(entry["name"] == "Empresas0.zip" for entry in failure["sources"])
    # The downloaded (copied-from-override) ZIP that FAILED extraction must
    # not linger on disk just because its own extraction failed -- nor may
    # any earlier successfully-processed part's ZIP.
    assert not any((root / "run" / "raw").glob("*.zip"))


def test_cli_rejects_malformed_zip_override(tmp_path):
    result = canonical_history_empresa.main(
        ["--month", "2026-04", "--root", str(tmp_path / "run"), "--zip", "not-a-valid-entry"]
    )
    assert result == 2


def test_cli_rejects_duplicate_zip_override_name(tmp_path):
    zip_path = tmp_path / "Empresas0.zip"
    zip_path.write_bytes(_zip_bytes([_row()]))
    result = canonical_history_empresa.main(
        [
            "--month",
            "2026-04",
            "--root",
            str(tmp_path / "run"),
            "--zip",
            f"Empresas0.zip={zip_path}",
            "--zip",
            f"Empresas0.zip={zip_path}",
        ]
    )
    assert result == 2
    assert not (tmp_path / "run").exists()


def test_run_rejects_override_name_outside_expected_part_set(tmp_path):
    zip_path = tmp_path / "fixture.zip"
    zip_path.write_bytes(_zip_bytes([_row()]))

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(
            f"network was touched ({request.method} {request.url}) despite an invalid "
            "override name that should fail before any I/O"
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="not in the expected empresa part set"):
            canonical_history_empresa.run_historical_empresa_dataset(
                "2026-04",
                tmp_path / "run",
                zip_overrides={"Empresas10.zip": zip_path},  # not a real part name
                client=client,
            )


def test_run_validates_every_override_before_any_download(tmp_path):
    """A bad override (missing file) among several valid ones must fail
    BEFORE preflight/download touches the network for the other parts --
    proven here by a client that raises if it is ever called at all."""
    root = tmp_path / "dataset"
    rows_by_part = {name: [_row(cnpj_basico=f"{i:08d}")] for i, name in enumerate(_ALL_PARTS)}
    overrides = _write_offline_dataset(root / "fixtures", rows_by_part)
    overrides["Empresas4.zip"] = root / "fixtures" / "does-not-exist.zip"  # never written

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(
            f"network was touched ({request.method} {request.url}) despite an invalid "
            "override that should fail before any download starts"
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FileNotFoundError, match="Empresas4.zip"):
            canonical_history_empresa.run_historical_empresa_dataset(
                "2026-04", root / "run", zip_overrides=overrides, client=client
            )
    assert not (root / "run").exists()


def test_run_rejects_override_that_is_not_a_valid_zip(tmp_path):
    root = tmp_path / "dataset"
    rows_by_part = {name: [_row(cnpj_basico=f"{i:08d}")] for i, name in enumerate(_ALL_PARTS)}
    overrides = _write_offline_dataset(root / "fixtures", rows_by_part)
    not_a_zip = root / "fixtures" / "Empresas4.zip"
    not_a_zip.write_bytes(b"not actually a zip file")
    overrides["Empresas4.zip"] = not_a_zip

    with pytest.raises(RuntimeError, match="not a valid ZIP"):
        canonical_history_empresa.run_historical_empresa_dataset(
            "2026-04", root / "run", zip_overrides=overrides
        )
    assert not (root / "run").exists()


def test_cli_offline_end_to_end(tmp_path, capsys):
    root = tmp_path / "dataset"
    rows_by_part = {name: [_row(cnpj_basico=f"{i:08d}")] for i, name in enumerate(_ALL_PARTS)}
    overrides = _write_offline_dataset(root / "fixtures", rows_by_part)

    argv = ["--month", "2026-04", "--root", str(root / "run"), "--sample-size", "5"]
    for name, path in overrides.items():
        argv += ["--zip", f"{name}={path}"]

    result = canonical_history_empresa.main(argv)
    assert result == 0
    out = capsys.readouterr().out
    assert "historical canonical empresa dataset written" in out
    assert (root / "run" / "evidence" / "empresa.history.json").exists()
