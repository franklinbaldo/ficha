"""Regression for descriptor recovery after partial RFB raw upload (#281/#390).

The production entrypoints, descriptor builder/loader and output preflight run
unchanged. Synthetic Parquet/files replace upstream RFB; a MockTransport and
IA SDK boundary adapter intercept HTTP PUT without contacting any server.
This does NOT prove that the real RFB bytes or IA SDK retries are pinned.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import duckdb
import httpx
import pytest

from ficha_etl import monthly_sharded as cli
from ficha_etl import publication_upload
from ficha_etl.monthly_source_bag_gate import EXPECTED_PARTS, _write_receipt
from ficha_etl.transform import _LOOKUP_KINDS


def _local_outputs(output_dir: Path) -> None:
    """Real Parquet files so build_production_descriptor and _load_descriptor run."""
    output_dir.mkdir(parents=True)
    with duckdb.connect() as connection:
        for name in cli._STANDARD_FILE_NAMES.values():
            target = output_dir / name
            if name.endswith(".parquet"):
                rows = 10 if name == "cnpjs.parquet" else 1
                literal = str(target).replace("'", "''")
                connection.execute(
                    f"COPY (SELECT n FROM range({rows}) t(n)) "
                    f"TO '{literal}' (FORMAT PARQUET)"
                )
            else:
                target.write_text("{}", encoding="utf-8")
        (output_dir / "lookups").mkdir()
        for kind in _LOOKUP_KINDS:
            target = output_dir / "lookups" / f"{kind}.parquet"
            literal = str(target).replace("'", "''")
            connection.execute(
                f"COPY (SELECT 1 AS n) TO '{literal}' (FORMAT PARQUET)"
            )


def _parsed_source_receipt(output_dir: Path) -> None:
    _write_receipt(
        cli._source_receipt_path(output_dir),
        {
            "schema": "ficha-parsed-rfb-key-multiset-v1",
            "month": "2026-07",
            "status": "ok",
            "identity_level": "parsed-duckdb-source-not-zip-custody",
            "parts": [
                {"zip_name": name, "parsed_csv_size": 100 + i, "parsed_csv_rows": 1}
                for i, name in enumerate(EXPECTED_PARTS)
            ],
            "parsed_source_rows": 10,
            "produced_rows": 10,
            "source_only_key_exists": False,
            "output_only_key_exists": False,
        },
    )


@pytest.mark.parametrize("raw_outcome", ["failed", "complete", "skipped"])
def test_raw_completion_controls_independent_upload_with_real_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw_outcome: str
) -> None:
    monkeypatch.setenv("FICHA_SOURCE_RUN_ID", "recovery:run-7")
    monkeypatch.setattr(cli, "_require_safe_legacy_shard_upload", lambda: None)
    monkeypatch.setattr(cli, "_require_secret", lambda _name: "offline-fixture")
    args = argparse.Namespace(
        month="2026-07",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        descriptor=tmp_path / "descriptor.json",
        skip_raw_upload=raw_outcome == "skipped",
        no_verify=True,
        verify_sample_size=10,
    )
    seen: list[tuple[str, str]] = []
    def receive(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        seen.append((request.method, request.url.path))
        return httpx.Response(201)
    transport = httpx.MockTransport(receive)

    def transform_fixture(*_args: object, **_kwargs: object) -> None:
        _local_outputs(args.output_dir)
        _parsed_source_receipt(args.output_dir)

    def raw_stream(*_args: object, **_kwargs: object) -> None:
        # The first raw PUT has begun; it may have changed remote state.
        pending = cli._load_descriptor(args.descriptor, args.month)
        assert pending["source_attestation"]["raw_upload_status"] == "pending"
        with httpx.Client(transport=transport) as client:
            response = client.put(
                "https://s3.us.archive.org/ficha-2026-07/raw/Estabelecimentos0.zip",
                content=b"raw",
            )
            assert response.status_code == 201
        if raw_outcome == "failed":
            raise RuntimeError("partial raw streaming failure")

    def fake_ia_upload(identifier: str, *, files: dict, **_kwargs: object) -> list:
        # This HTTP test adapter models the IA upload boundary; it is not
        # internetarchive's real transport or an authenticated remote PUT.
        results = []
        with httpx.Client(transport=transport) as client:
            for name, stream in files.items():
                result = client.put(
                    f"https://s3.us.archive.org/{identifier}/{name}",
                    content=stream.read(),
                )
                results.append(SimpleNamespace(status_code=result.status_code))
        return results

    def actual_submit(*a: object, **kw: object):
        return publication_upload.submit_outputs_fail_closed(
            *a,
            **kw,
            fetch_metadata=lambda: {"files": []},
            head_status=lambda _url: 404,
            attempts=1,
        )

    monkeypatch.setattr(cli.transform, "transform_snapshot", transform_fixture)
    # The key gate was separately exercised with real DuckDB in #368/#385.
    # This regression isolates the raw completion to upload-outputs boundary.
    monkeypatch.setattr(cli, "run_monthly_gate", lambda *_args: None)
    monkeypatch.setattr(cli.upload, "stream_raw_zips_to_ia", raw_stream)
    monkeypatch.setattr(cli, "submit_outputs_fail_closed", actual_submit)
    monkeypatch.setattr(publication_upload.ia, "upload", fake_ia_upload)

    if raw_outcome == "failed":
        with pytest.raises(RuntimeError, match="partial raw"):
            cli.command_produce(args)
    else:
        cli.command_produce(args)

    descriptor = cli._load_descriptor(args.descriptor, args.month)
    expected_state = {
        "failed": "failed",
        "complete": "complete",
        "skipped": "pending",
    }[raw_outcome]
    assert descriptor["source_attestation"]["raw_upload_status"] == expected_state
    raw_requests = [url for _, url in seen if "/raw/" in url]
    derived_requests = [url for _, url in seen if "/raw/" not in url]
    assert len(raw_requests) == (0 if raw_outcome == "skipped" else 1)
    assert derived_requests == []

    output_args = argparse.Namespace(
        month=args.month,
        descriptor=args.descriptor,
        output_dir=args.output_dir,
    )
    if raw_outcome == "complete":
        cli.command_upload_outputs(output_args)
        derived_requests = [url for _, url in seen if "/raw/" not in url]
        assert len(derived_requests) == len(cli._STANDARD_FILE_NAMES) + len(_LOOKUP_KINDS)
    else:
        with pytest.raises(ValueError, match="raw source upload did not complete"):
            cli.command_upload_outputs(output_args)
        # A valid source receipt and real descriptor must not bypass raw status.
        assert [url for _, url in seen if "/raw/" not in url] == []
