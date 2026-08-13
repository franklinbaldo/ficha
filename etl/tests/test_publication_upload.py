from __future__ import annotations

from pathlib import Path

import pytest

from ficha_etl import publication_upload as publication


def _entry(name: str, digit: str, size: int = 1) -> dict:
    return {
        "url": f"https://example.test/{name}",
        "size": size,
        "sha1": digit * 40,
        "sha256": digit * 64,
    }


def _descriptor() -> dict:
    files = {
        "cnpjs": _entry("cnpjs.parquet", "1"),
        "cnpj_contatos": _entry("cnpj_contatos.parquet", "2"),
        "cnpj_cnaes": _entry("cnpj_cnaes.parquet", "3"),
        "raizes": _entry("raizes.parquet", "4"),
        "socios": _entry("socios.parquet", "5"),
        "enderecos": _entry("enderecos.parquet", "6"),
        "pessoas": _entry("pessoas.parquet", "7"),
        "lookups": _entry("lookups.json", "8"),
    }
    lookups = {
        kind: _entry(f"lookups/{kind}.parquet", digit)
        for kind, digit in zip(
            ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"),
            "abcdef",
            strict=True,
        )
    }
    return {"date": "2026-06", "files": files, "lookups": lookups}


def _metadata(descriptor: dict, *, omit: set[str] = frozenset()) -> dict:
    return {
        "files": [
            {"name": name, "size": str(entry["size"]), "sha1": entry["sha1"]}
            for name, entry in publication.descriptor_file_entries(descriptor).items()
            if name not in omit
        ]
    }


def test_exact_catalog_identity_is_reused_without_head() -> None:
    descriptor = _descriptor()

    def unexpected_head(_url: str) -> int:
        raise AssertionError("HEAD must not run for catalogued identities")

    plan = publication.classify_output_upload(
        descriptor,
        _metadata(descriptor),
        head_status=unexpected_head,
    )
    assert plan.upload == ()
    assert len(plan.reuse) == 14
    assert plan.pending == ()
    assert plan.mismatches == ()


def test_catalog_absence_only_allows_put_after_head_404() -> None:
    descriptor = _descriptor()
    missing = {"cnpjs.parquet"}
    plan = publication.classify_output_upload(
        descriptor,
        _metadata(descriptor, omit=missing),
        head_status=lambda _url: 404,
    )
    assert plan.upload == ("cnpjs.parquet",)
    assert plan.pending == ()
    assert plan.mismatches == ()


def test_served_object_missing_from_catalog_is_pending_not_uploadable() -> None:
    descriptor = _descriptor()
    plan = publication.classify_output_upload(
        descriptor,
        _metadata(descriptor, omit={"cnpjs.parquet"}),
        head_status=lambda _url: 200,
    )
    assert plan.upload == ()
    assert plan.pending == ("cnpjs.parquet",)


def test_catalog_mismatch_is_hard_failure() -> None:
    descriptor = _descriptor()
    metadata = _metadata(descriptor)
    target = next(entry for entry in metadata["files"] if entry["name"] == "cnpjs.parquet")
    target["sha1"] = "0" * 40

    with pytest.raises(publication.OutputPublishError, match="diverges"):
        publication.wait_for_safe_output_plan(
            descriptor,
            fetch_metadata=lambda: metadata,
            head_status=lambda _url: 404,
            attempts=1,
            sleep=lambda _seconds: None,
        )


def test_eventual_catalog_visibility_reuses_without_overwrite() -> None:
    descriptor = _descriptor()
    states = iter(
        [
            _metadata(descriptor, omit={"cnpjs.parquet"}),
            _metadata(descriptor),
        ]
    )
    sleeps: list[float] = []
    plan = publication.wait_for_safe_output_plan(
        descriptor,
        fetch_metadata=lambda: next(states),
        head_status=lambda _url: 200,
        attempts=2,
        interval_s=3,
        sleep=sleeps.append,
    )
    assert plan.upload == ()
    assert len(plan.reuse) == 14
    assert sleeps == [3]


def test_local_bytes_must_still_match_descriptor(tmp_path: Path) -> None:
    descriptor = _descriptor()
    entries = publication.descriptor_file_entries(descriptor)
    for name, entry in entries.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        entry["size"] = 1
        entry["sha1"] = publication._sha1_file(path)

    paths = publication.local_output_paths(tmp_path, descriptor)
    assert set(paths) == set(entries)

    # Mesmo tamanho, bytes diferentes: size sozinho não pode autorizar PUT.
    (tmp_path / "cnpjs.parquet").write_bytes(b"y")
    with pytest.raises(publication.OutputPublishError, match="local bytes changed"):
        publication.local_output_paths(tmp_path, descriptor)
