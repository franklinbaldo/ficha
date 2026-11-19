from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ficha_etl import publication_upload as p


def _fixtures(root: Path):
    desc = {"files": {}, "lookups": {}}
    expected = {}
    for key, name in p._STANDARD_FILES.items():
        data = ("original:" + name).encode()
        file = root / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(data)
        desc["files"][key] = {
            "url": "https://example.test/" + name,
            "size": len(data),
            "sha1": hashlib.sha1(data, usedforsecurity=False).hexdigest(),
        }
        expected[name] = data
    name = "lookups/cnaes.parquet"
    data = b"cnae-lookup-original"
    (root / "lookups").mkdir(exist_ok=True)
    (root / name).write_bytes(data)
    desc["lookups"]["cnaes"] = {
        "url": "https://example.test/" + name,
        "size": len(data),
        "sha1": hashlib.sha1(data, usedforsecurity=False).hexdigest(),
    }
    expected[name] = data
    return desc, expected


def _catalog(desc, missing=("cnpjs.parquet",)):
    return {
        "files": [
            {"name": n, "size": e["size"], "sha1": e["sha1"]}
            for n, e in p.descriptor_file_entries(desc).items()
            if n not in missing
        ]
    }


def _run(root, desc, *, metadata, head):
    return p.submit_outputs_fail_closed(
        "2026-06",
        root,
        desc,
        access_key="offline",
        secret_key="offline",
        fetch_metadata=metadata,
        head_status=head,
        attempts=1,
    )


def test_preflight_local_mutation_same_size_refuses_put(tmp_path, monkeypatch):
    desc, payloads = _fixtures(tmp_path)
    target = tmp_path / "cnpjs.parquet"
    writes = []
    monkeypatch.setattr(p.ia, "upload", lambda *args, **kwargs: writes.append((args, kwargs)))
    heads = 0

    def head(_url):
        nonlocal heads
        heads += 1
        if heads == 1:
            target.write_bytes(b"X" * len(payloads["cnpjs.parquet"]))
        return 404

    with pytest.raises(p.OutputPublishError, match="snapshot|changed"):
        _run(tmp_path, desc, metadata=lambda: _catalog(desc), head=head)
    assert heads >= 1
    assert writes == []


def test_post_snapshot_path_replacement_uploads_original_handle(tmp_path, monkeypatch):
    desc, payloads = _fixtures(tmp_path)
    target = tmp_path / "cnpjs.parquet"
    observed = []
    heads = 0

    def head(_url):
        nonlocal heads
        heads += 1
        if heads == 2:
            replacement = tmp_path / "replacement.parquet"
            replacement.write_bytes(b"Y" * len(payloads["cnpjs.parquet"]))
            replacement.replace(target)
        return 404

    def upload(_item, *, files, **_kwargs):
        assert list(files) == ["cnpjs.parquet"]
        assert not isinstance(files["cnpjs.parquet"], (str, Path))
        observed.append(files["cnpjs.parquet"].read())
        return [type("Response", (), {"status_code": 201, "url": "offline"})()]

    monkeypatch.setattr(p.ia, "upload", upload)
    plan = _run(tmp_path, desc, metadata=lambda: _catalog(desc), head=head)
    assert heads == 2
    assert plan.upload == ("cnpjs.parquet",)
    assert observed == [payloads["cnpjs.parquet"]]
    assert target.read_bytes() != payloads["cnpjs.parquet"]


@pytest.mark.parametrize("identical", [True, False])
def test_remote_race_during_snapshot_causes_reuse_or_refusal(tmp_path, monkeypatch, identical):
    desc, _ = _fixtures(tmp_path)
    calls = 0
    uploads = []
    monkeypatch.setattr(p.ia, "upload", lambda *args, **kw: uploads.append((args, kw)))

    def metadata():
        nonlocal calls
        calls += 1
        catalog = _catalog(desc) if calls == 1 else _catalog(desc, missing=())
        if calls == 2 and not identical:
            row = next(r for r in catalog["files"] if r["name"] == "cnpjs.parquet")
            row["sha1"] = "f" * 40
        return catalog

    if identical:
        result = _run(tmp_path, desc, metadata=metadata, head=lambda _: 404)
        assert "cnpjs.parquet" in result.reuse
    else:
        with pytest.raises(p.OutputPublishError, match="diverges"):
            _run(tmp_path, desc, metadata=metadata, head=lambda _: 404)
    assert calls == 2
    assert uploads == []


def test_snapshot_tempfile_failure_never_uploads(tmp_path, monkeypatch):
    desc, _ = _fixtures(tmp_path)
    uploads = []
    monkeypatch.setattr(p.ia, "upload", lambda *args, **kwargs: uploads.append((args, kwargs)))

    def no_space(*args, **kwargs):
        raise OSError("no local scratch capacity")

    monkeypatch.setattr(p.tempfile, "TemporaryFile", no_space)
    with pytest.raises(OSError, match="scratch capacity"):
        _run(tmp_path, desc, metadata=lambda: _catalog(desc), head=lambda _: 404)
    assert uploads == []


def test_healthy_upload_uses_expected_stream(tmp_path, monkeypatch):
    desc, payloads = _fixtures(tmp_path)
    received = {}

    def upload(_item, *, files, **_kwargs):
        received.update({name: stream.read() for name, stream in files.items()})
        return [type("Response", (), {"status_code": 200, "url": "offline"})()]

    monkeypatch.setattr(p.ia, "upload", upload)
    plan = _run(tmp_path, desc, metadata=lambda: _catalog(desc), head=lambda _: 404)
    assert plan.upload == ("cnpjs.parquet",)
    assert received == {"cnpjs.parquet": payloads["cnpjs.parquet"]}


def test_newly_absent_remote_name_after_snapshot_requires_retry(tmp_path, monkeypatch):
    desc, _ = _fixtures(tmp_path)
    calls = 0
    uploads = []
    monkeypatch.setattr(p.ia, "upload", lambda *args, **kw: uploads.append((args, kw)))

    def metadata():
        nonlocal calls
        calls += 1
        if calls == 1:
            return _catalog(desc)
        return _catalog(desc, missing=("cnpjs.parquet", "socios.parquet"))

    with pytest.raises(p.OutputPublishError, match="plan changed after snapshot"):
        _run(tmp_path, desc, metadata=metadata, head=lambda _: 404)
    assert calls == 2
    assert uploads == []


def test_all_remote_files_reused_never_touch_tempfile(tmp_path, monkeypatch):
    desc, _ = _fixtures(tmp_path)

    def unexpected(*_args, **_kwargs):
        raise AssertionError("No snapshot or upload expected for fully reused outputs")

    monkeypatch.setattr(p.ia, "upload", unexpected)
    monkeypatch.setattr(p.tempfile, "TemporaryFile", unexpected)
    result = _run(tmp_path, desc, metadata=lambda: _catalog(desc, missing=()), head=unexpected)
    assert len(result.reuse) == len(p.descriptor_file_entries(desc))
    assert result.upload == ()
