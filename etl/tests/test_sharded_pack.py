from __future__ import annotations

import json
import zipfile

import duckdb
import pytest

from ficha_etl.sharded_pack import ShardGeometry, ShardPackSession


LOOKUP_KINDS = ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes")


def _write_fixture(root, bases: list[str], *, porte_codes: list[str | None] | None = None) -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE bases (cnpj_base VARCHAR, porte_empresa VARCHAR)")
    codes = ["01"] * len(bases) if porte_codes is None else porte_codes
    con.executemany("INSERT INTO bases VALUES (?, ?)", list(zip(bases, codes, strict=True)))

    con.execute(f"""
        COPY (
            SELECT
                cnpj_base,
                'RAZAO ' || cnpj_base AS razao_social,
                'RAZAO ' || cnpj_base AS razao_social_normalizada,
                '2062' AS natureza_juridica_codigo,
                porte_empresa,
                1000.0 AS capital_social,
                NULL::VARCHAR AS ente_federativo_responsavel,
                1 AS qtd_estabelecimentos,
                1 AS qtd_estabelecimentos_ativos
            FROM bases
        ) TO '{root}/raizes.parquet' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT
                cnpj_base,
                NULL::VARCHAR AS cnpj_ordem,
                NULL::VARCHAR AS cnpj_dv,
                NULL::VARCHAR AS identificador_matriz_filial,
                NULL::VARCHAR AS nome_fantasia,
                NULL::VARCHAR AS situacao_cadastral,
                NULL::VARCHAR AS data_situacao_cadastral,
                NULL::VARCHAR AS motivo_situacao_cadastral_codigo,
                NULL::VARCHAR AS situacao_especial,
                NULL::VARCHAR AS data_situacao_especial,
                NULL::VARCHAR AS data_inicio_atividade,
                NULL::VARCHAR AS cnae_principal_codigo,
                []::VARCHAR[] AS cnae_secundario_codigos,
                NULL::VARCHAR AS tipo_logradouro,
                NULL::VARCHAR AS logradouro,
                NULL::VARCHAR AS numero,
                NULL::VARCHAR AS complemento,
                NULL::VARCHAR AS bairro,
                NULL::VARCHAR AS cep,
                NULL::VARCHAR AS uf,
                NULL::VARCHAR AS municipio_codigo,
                NULL::VARCHAR AS nome_cidade_exterior,
                NULL::VARCHAR AS pais_codigo,
                NULL::VARCHAR AS ddd_1,
                NULL::VARCHAR AS telefone_1,
                NULL::VARCHAR AS ddd_2,
                NULL::VARCHAR AS telefone_2,
                NULL::VARCHAR AS ddd_fax,
                NULL::VARCHAR AS fax,
                NULL::VARCHAR AS correio_eletronico,
                NULL::VARCHAR AS opcao_simples,
                NULL::VARCHAR AS data_opcao_simples,
                NULL::VARCHAR AS data_exclusao_simples,
                NULL::VARCHAR AS opcao_mei,
                NULL::VARCHAR AS data_opcao_mei,
                NULL::VARCHAR AS data_exclusao_mei
            FROM bases
        ) TO '{root}/cnpjs.parquet' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT
                cnpj_base,
                NULL::VARCHAR AS tipo,
                NULL::VARCHAR AS nome_socio_razao_social,
                NULL::VARCHAR AS cpf_mascarado,
                NULL::VARCHAR AS cnpj_socio,
                NULL::VARCHAR AS qualificacao_codigo,
                NULL::VARCHAR AS data_entrada_sociedade,
                NULL::VARCHAR AS pais_codigo,
                NULL::VARCHAR AS faixa_etaria,
                NULL::VARCHAR AS representante_legal_cpf,
                NULL::VARCHAR AS representante_legal_nome,
                NULL::VARCHAR AS representante_legal_qualificacao_codigo
            FROM bases
        ) TO '{root}/socios.parquet' (FORMAT PARQUET)
    """)

    lookups = root / "lookups"
    lookups.mkdir()
    for kind in LOOKUP_KINDS:
        con.execute(f"""
            COPY (SELECT '1' AS codigo, 'DESC' AS descricao)
            TO '{lookups}/{kind}.parquet' (FORMAT PARQUET)
        """)
    con.close()


@pytest.mark.parametrize(
    ("digits", "prefix", "cnpj", "bounds"),
    [
        (2, "07", "07123456", ("07000000", "08000000")),
        (3, "071", "07123456", ("07100000", "07200000")),
    ],
)
def test_geometry_is_explicit_and_validated(digits, prefix, cnpj, bounds):
    geometry = ShardGeometry(digits)
    assert geometry.shard_of(cnpj) == prefix
    assert geometry.shard_of(int(cnpj)) == prefix
    assert geometry.range_bounds(prefix) == bounds
    assert geometry.shard_name(prefix) == f"companies-{prefix}.zip"


@pytest.mark.parametrize("digits", [1, 4, 8])
def test_unmeasured_geometries_are_rejected(digits):
    with pytest.raises(ValueError, match="2 or 3"):
        ShardGeometry(digits)


def test_invalid_roots_and_prefixes_fail_high():
    geometry = ShardGeometry(2)
    for root in ("7123456", "abcdefgh", "123456789", -1, 100_000_000):
        with pytest.raises((TypeError, ValueError)):
            geometry.shard_of(root)
    for prefix in ("7", "007", "ab", " 7"):
        with pytest.raises((TypeError, ValueError)):
            geometry.validate_prefix(prefix)


@pytest.mark.parametrize(("digits", "prefix"), [(2, "07"), (3, "071")])
def test_one_shard_is_self_describing_and_materialized(tmp_path, digits, prefix):
    _write_fixture(tmp_path, ["07123456", "42000001"])
    geometry = ShardGeometry(digits)
    out = tmp_path / "out"

    session = ShardPackSession("2026-05", geometry, parquets_base=str(tmp_path))
    with session:
        spec = session.materialization_spec(
            prefix,
            input_sha1s={
                "cnpjs.parquet": "1" * 40,
                "raizes.parquet": "2" * 40,
                "socios.parquet": "3" * 40,
            },
        )
        artifact = session.pack(prefix, out, materialization=spec)

    assert artifact.count == 1
    assert artifact.path.name == f"companies-{prefix}.zip"
    assert artifact.size_bytes == artifact.path.stat().st_size
    assert artifact.materialization_id == spec.materialization_id()
    assert not artifact.path.with_name(artifact.path.name + ".part").exists()

    with zipfile.ZipFile(artifact.path) as zf:
        names = set(zf.namelist())
        assert "07/123/456.pb" in names
        assert "42/000/001.pb" not in names
        assert "_schema.desc" in names
        assert "_schema.proto" in names
        assert all(f"_lookups/{kind}.pb" in names for kind in LOOKUP_KINDS)
        meta = json.loads(zf.read("_meta.json"))

    assert meta["snapshot_month"] == "2026-05"
    assert meta["count"] == 1
    assert meta["materialization"]["id"] == spec.materialization_id()
    assert meta["materialization"]["spec"]["range"] == {
        "kind": "cnpj_base_prefix_or_base36_mod_v1",
        "value": prefix,
    }
    assert meta["artifact"] is None

    with pytest.raises(RuntimeError, match="inside a with block"):
        session.pack(prefix, out, materialization=spec)


def test_materialization_must_match_the_exact_shard(tmp_path):
    _write_fixture(tmp_path, ["07123456"])
    geometry = ShardGeometry(2)
    with ShardPackSession("2026-05", geometry, parquets_base=str(tmp_path)) as session:
        wrong = session.materialization_spec("08", input_sha1s={"cnpjs.parquet": "a" * 40})
        with pytest.raises(ValueError, match="does not match shard"):
            session.pack("07", tmp_path / "out", materialization=wrong)


def test_real_v11_shard_session_selects_alpha_root_and_plans_versioned_name(tmp_path):
    """Full local DuckDB -> pack -> receipt -> opt-in plan, no network or upload."""
    from ficha_etl.proto.ficha.v1.company_pb2 import Company
    from ficha_etl.shard_transfer import prepare_one_shard, plan_opt_in_v11_shard

    _write_fixture(tmp_path, ["42000001", "CA000001"])
    pins = {
        "cnpjs.parquet": "1" * 40,
        "raizes.parquet": "2" * 40,
        "socios.parquet": "3" * 40,
    }
    geometry = ShardGeometry(2)
    assert geometry.shard_of("42000001") == "42"  # numeric v1 bucket
    assert geometry.shard_of("CA000001") == "42"  # base36(CA) % 100
    with ShardPackSession("2026-08", geometry, parquets_base=str(tmp_path)) as session:
        prepared = prepare_one_shard(session, "42", tmp_path / "out", pinned_inputs=pins)
        planned = plan_opt_in_v11_shard(session, prepared, pinned_inputs=pins)

    assert planned.name == "wire-1.1/companies-42.zip"
    assert prepared.path.name == "companies-42.zip"
    assert prepared.size == planned.size
    assert prepared.sha1 == planned.sha1
    assert prepared.materialization_id == planned.materialization_id
    assert not (tmp_path / "out" / "wire-1.1").exists()

    with zipfile.ZipFile(prepared.path) as archive:
        assert archive.testzip() is None
        names = set(archive.namelist())
        assert {"42/000/001.pb", "CA/000/001.pb", "_schema.desc"}.issubset(names)
        company = Company.FromString(archive.read("CA/000/001.pb"))
        assert company.cnpj_base_text == "CA000001"
        assert company.snapshot_yyyymm == 202608
        legacy = Company.FromString(archive.read("42/000/001.pb"))
        assert legacy.cnpj_base == 42000001
        meta = json.loads(archive.read("_meta.json"))
        assert meta["count"] == 2
        assert meta["schema_version"] == "1.1.0"
        assert meta["materialization"]["id"] == planned.materialization_id


@pytest.fixture
def local_prepare(tmp_path, monkeypatch):
    """Real command/session/SQL/ZIP; replace only the remote Parquet location."""
    import argparse

    from ficha_etl import monthly_sharded
    from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY

    from .test_monthly_sharded import _descriptor

    source = tmp_path / "source"
    source.mkdir()
    _write_fixture(source, ["42000001", "43000001"])
    descriptor = tmp_path / "descriptor.json"
    payload = _descriptor()
    descriptor.write_text(json.dumps(payload), encoding="utf-8")
    out = tmp_path / "out"
    session = ShardPackSession("2026-06", PUBLIC_COMPANIES_GEOMETRY, parquets_base=str(source))
    pins = monthly_sharded.materialization_inputs_from_descriptor(payload)
    expected = {
        prefix: session.materialization_spec(prefix, input_sha1s=pins).materialization_id()
        for prefix in ("42", "43")
    }

    def local_session(month, geometry, **kwargs):
        return ShardPackSession(month, geometry, parquets_base=str(source), **kwargs)

    monkeypatch.setattr(monthly_sharded, "ShardPackSession", local_session)

    def prepare(prefix, receipt, *, output_dir=out):
        monthly_sharded.command_prepare_shard(
            argparse.Namespace(
                month="2026-06",
                prefix=prefix,
                descriptor=str(descriptor),
                output_dir=str(output_dir),
                receipt=str(receipt),
            )
        )

    return out, prepare, expected


def _check_shard_zip(out, prefix, expected):
    from collections import Counter

    from ficha_etl.proto.ficha.v1.company_pb2 import Company

    path = out / f"companies-{prefix}.zip"
    member = f"{prefix}/000/001.pb"
    names = ["_schema.desc", "_schema.proto", "_meta.json", member]
    names.extend(f"_lookups/{kind}.pb" for kind in LOOKUP_KINDS)
    with zipfile.ZipFile(path) as archive:
        if archive.testzip() is not None:
            pytest.fail("ZIP CRC mismatch")
        if Counter(archive.namelist()) != Counter(names):
            pytest.fail("unexpected physical ZIP members")
        meta = json.loads(archive.read("_meta.json"))
        if (
            meta["materialization"]["id"] != expected[prefix]
            or meta["count"] != 1
            or meta["snapshot_month"] != "2026-06"
        ):
            pytest.fail("unexpected shard metadata")
        company = Company.FromString(archive.read(member))
        if company.cnpj_base != int(prefix + "000001") or company.snapshot_yyyymm != 202606:
            pytest.fail("unexpected Company identity or snapshot")
    return path.read_bytes()


def _check_shard_receipt(out, prefix, receipt, expected):
    import hashlib

    from ficha_etl.monthly_sharded import _load_receipt

    data = _check_shard_zip(out, prefix, expected)
    payload = _load_receipt(receipt, "2026-06", prefix)
    if (
        payload["size"] != len(data)
        or payload["sha1"] != hashlib.sha1(data, usedforsecurity=False).hexdigest()
        or payload["materialization_id"] != expected[prefix]
    ):
        pytest.fail("receipt does not describe expected final ZIP")
    return data


@pytest.mark.parametrize(
    "relative",
    [
        "companies-43.zip",
        "companies-42.zip.part/receipt.json",
        "companies-43.zip.part/receipt.json",
    ],
)
def test_prepare_refuses_reserved_receipt_before_pack(local_prepare, relative):
    out, prepare, _ = local_prepare
    with pytest.raises(ValueError, match="reserved shard namespace"):
        prepare("42", out / relative)
    if out.exists():
        pytest.fail("reserved receipt rejection wrote to output")


@pytest.mark.parametrize("argument", ["receipt", "output_dir"])
def test_prepare_refuses_parent_traversal(local_prepare, tmp_path, argument):
    out, prepare, _ = local_prepare
    out.mkdir()
    alias = out / "companies-43.zip.part" / ".."
    with pytest.raises(ValueError, match="parent traversal"):
        if argument == "receipt":
            prepare("42", alias / "a.json")
        else:
            prepare("42", tmp_path / "a.json", output_dir=alias)
    if list(out.iterdir()) or (tmp_path / "a.json").exists():
        pytest.fail("traversal rejection created output or receipt")


def test_prepare_windows_case_alias(local_prepare):
    import os

    if os.name != "nt":
        pytest.skip("Win32 pathname semantics")
    out, prepare, _ = local_prepare
    with pytest.raises(ValueError, match="reserved shard namespace"):
        prepare("42", out / "COMPANIES-43.ZIP")
    if out.exists():
        pytest.fail("alias rejection wrote to output")


@pytest.mark.parametrize("shared_receipt", [False, True])
def test_prepare_competing_same_and_other_prefix(
    local_prepare, tmp_path, monkeypatch, shared_receipt
):
    from ficha_etl import sharded_pack

    out, prepare, expected = local_prepare
    original = sharded_pack._iter_rows_for_range
    entered = False
    a = tmp_path / "a.json"
    b = a if shared_receipt else tmp_path / "b.json"
    rival = tmp_path / "rival.json"
    frozen_b = None
    receipt_b = None

    def interleave(*args, **kwargs):
        nonlocal entered, frozen_b, receipt_b
        if not entered:
            entered = True
            with pytest.raises(FileExistsError):
                prepare("42", rival)
            with pytest.raises(ValueError, match="reserved shard namespace"):
                prepare("43", out / "companies-42.zip")
            prepare("43", b)
            frozen_b = _check_shard_receipt(out, "43", b, expected)
            receipt_b = b.read_bytes()
        yield from original(*args, **kwargs)

    monkeypatch.setattr(sharded_pack, "_iter_rows_for_range", interleave)
    if shared_receipt:
        with pytest.raises(FileExistsError):
            prepare("42", a)
        frozen_a = _check_shard_zip(out, "42", expected)
    else:
        prepare("42", a)
        frozen_a = _check_shard_receipt(out, "42", a, expected)
    if frozen_b != _check_shard_receipt(out, "43", b, expected) or receipt_b != b.read_bytes():
        pytest.fail("continuation of A changed B")
    receipt_a = a.read_bytes()
    with pytest.raises(FileExistsError):
        prepare("42", rival)
    with pytest.raises(FileExistsError):
        prepare("42", a)
    if not entered or rival.exists() or a.read_bytes() != receipt_a:
        pytest.fail("rival attempt changed receipt state")
    if frozen_a != _check_shard_zip(out, "42", expected):
        pytest.fail("retry replaced A")
    if frozen_b != _check_shard_receipt(out, "43", b, expected) or receipt_b != b.read_bytes():
        pytest.fail("retry replaced B")
    if list(out.glob("*.part")):
        pytest.fail("owned staging was not cleaned")


@pytest.mark.parametrize("legacy", ["file", "directory", "dangling-final"])
def test_prepare_preserves_existing_stage_or_final(local_prepare, tmp_path, legacy):
    out, prepare, _ = local_prepare
    out.mkdir()
    stage = out / "companies-42.zip.part"
    marker = stage
    if legacy == "directory":
        stage.mkdir()
        marker = stage / "owner"
    elif legacy == "dangling-final":
        marker = out / "companies-42.zip"
        try:
            marker.symlink_to(tmp_path / "missing")
        except (OSError, NotImplementedError):
            pytest.skip("symlink unavailable")
    if legacy != "dangling-final":
        marker.write_bytes(b"other producer")
    with pytest.raises(FileExistsError):
        prepare("42", tmp_path / "receipt.json")
    if legacy == "dangling-final":
        if not marker.is_symlink() or stage.exists():
            pytest.fail("dangling final or owned stage mishandled")
    elif marker.read_bytes() != b"other producer":
        pytest.fail("pre-existing staging was modified")


def test_prepare_failure_before_publish_allows_retry(local_prepare, tmp_path, monkeypatch):
    from ficha_etl import sharded_pack

    out, prepare, expected = local_prepare
    a = tmp_path / "a.json"
    receipt = tmp_path / "b.json"
    prepare("42", a)
    frozen_a = _check_shard_receipt(out, "42", a, expected)
    original = sharded_pack._iter_rows_for_range

    def broken(*args, **kwargs):
        yield from ()
        raise OSError("injected pack failure")

    monkeypatch.setattr(sharded_pack, "_iter_rows_for_range", broken)
    with pytest.raises(OSError, match="injected pack failure"):
        prepare("43", receipt)
    if receipt.exists() or (out / "companies-43.zip").exists() or list(out.glob("*.part")):
        pytest.fail("failed pack exposed artifacts")
    if frozen_a != _check_shard_receipt(out, "42", a, expected):
        pytest.fail("failure of B changed A")
    monkeypatch.setattr(sharded_pack, "_iter_rows_for_range", original)
    prepare("43", receipt)
    _check_shard_receipt(out, "43", receipt, expected)
    if frozen_a != _check_shard_receipt(out, "42", a, expected):
        pytest.fail("retry of B changed A")


@pytest.mark.parametrize("state", ["absent", "partial", "complete"])
def test_prepare_receipt_failure_preserves_final_zip(local_prepare, tmp_path, monkeypatch, state):
    from pathlib import Path

    out, prepare, expected = local_prepare
    receipt = tmp_path / "a.json"
    original = Path.open
    frozen = []

    class FailingWriter:
        def __enter__(self):
            if state == "absent":
                raise OSError("injected receipt failure")
            self.stream = original(receipt, "x", encoding="utf-8")
            return self

        def write(self, text):
            self.stream.write(text if state == "complete" else text[: len(text) // 2])
            self.stream.flush()
            raise OSError("injected receipt failure")

        def __exit__(self, *exc):
            self.stream.close()

    def open_with_failure(path, mode="r", *args, **kwargs):
        if path == receipt and mode == "x":
            frozen.append((out / "companies-42.zip").read_bytes())
            return FailingWriter()
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_with_failure)
    with pytest.raises(OSError, match="injected receipt failure"):
        prepare("42", receipt)
    monkeypatch.setattr(Path, "open", original)
    final = out / "companies-42.zip"
    if len(frozen) != 1 or _check_shard_zip(out, "42", expected) != frozen[0]:
        pytest.fail("receipt failure changed final ZIP")
    if state == "absent":
        if receipt.exists():
            pytest.fail("receipt unexpectedly exists")
    elif state == "partial":
        with pytest.raises(json.JSONDecodeError):
            json.loads(receipt.read_text(encoding="utf-8"))
    else:
        _check_shard_receipt(out, "42", receipt, expected)
    if list(out.glob("*.part")):
        pytest.fail("owned staging was not cleaned")
    before = receipt.read_bytes() if receipt.exists() else None
    with pytest.raises(FileExistsError):
        prepare("42", tmp_path / "new.json")
    if final.read_bytes() != frozen[0]:
        pytest.fail("retry changed final ZIP")
    after = receipt.read_bytes() if receipt.exists() else None
    if before != after:
        pytest.fail("retry changed residual receipt")


def test_prepare_existing_receipt_does_not_pack(local_prepare, tmp_path):
    out, prepare, _ = local_prepare
    receipt = tmp_path / "existing.json"
    receipt.write_bytes(b"other producer")
    with pytest.raises(FileExistsError, match="receipt already exists"):
        prepare("42", receipt)
    if out.exists() or receipt.read_bytes() != b"other producer":
        pytest.fail("preflight changed artifacts")


def test_prepare_resolved_alias_does_not_pack(local_prepare, tmp_path):
    out, prepare, _ = local_prepare
    out.mkdir()
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(out, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("directory symlink unavailable")
    with pytest.raises(ValueError, match="reserved shard namespace"):
        prepare("42", alias / "companies-43.zip")
    if list(out.iterdir()):
        pytest.fail("resolved alias rejection wrote to output")


def test_both_packers_decode_same_rfb_porte_from_local_parquet(tmp_path):
    """Both public pack paths read the same synthetic VARCHAR Parquets."""
    import hashlib

    from ficha_etl.pack import pack_from_parquets
    from ficha_etl.proto.ficha.v1.company_pb2 import Company

    codes = ["00", "01", "03", "05", "02", None, "1", "99"]
    expected = dict(zip(range(42000001, 42000009), [1, 2, 3, 5, 0, 0, 0, 0], strict=True))
    _write_fixture(tmp_path, [str(root) for root in expected], porte_codes=codes)
    with duckdb.connect() as con:
        column_type = con.execute(
            "SELECT typeof(porte_empresa) FROM read_parquet(?) LIMIT 1",
            [str(tmp_path / "raizes.parquet")],
        ).fetchone()[0]
    if column_type != "VARCHAR":
        pytest.fail("fixture must exercise lexical RFB values")
    pins = {
        path.relative_to(tmp_path).as_posix(): hashlib.sha1(
            path.read_bytes(), usedforsecurity=False
        ).hexdigest()
        for path in sorted(tmp_path.rglob("*.parquet"))
    }
    mono = tmp_path / "companies.zip"
    pack_from_parquets("2026-08", mono, parquets_base=str(tmp_path))
    with ShardPackSession("2026-08", ShardGeometry(2), parquets_base=str(tmp_path)) as session:
        spec = session.materialization_spec("42", input_sha1s=pins)
        packed = session.pack("42", tmp_path / "out", materialization=spec)
    descriptors = []
    for path in (mono, packed.path):
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                pytest.fail("invalid ZIP CRC")
            descriptor = archive.read("_schema.desc")
            descriptors.append(descriptor)
            meta = json.loads(archive.read("_meta.json"))
            if meta["count"] != len(expected) or meta["schema_version"] != "1.1.0":
                pytest.fail("unexpected metadata")
            if meta["schema_sha256"] != hashlib.sha256(descriptor).hexdigest():
                pytest.fail("descriptor hash mismatch")
            members = [n for n in archive.namelist() if n.endswith(".pb") and not n.startswith("_")]
            if len(members) != len(expected):
                pytest.fail("unexpected member multiplicity")
            actual = {}
            for name in members:
                company = Company.FromString(archive.read(name))
                if company.snapshot_yyyymm != 202608:
                    pytest.fail("unexpected snapshot")
                if company.cnpj_base in actual:
                    pytest.fail("duplicate company")
                actual[company.cnpj_base] = company.porte_empresa
            if actual != expected:
                pytest.fail(f"{path.name}: {actual!r} != {expected!r}")
            if path == packed.path:
                if meta["materialization"] != spec.meta_payload():
                    pytest.fail("shard materialization differs from expected spec")
                if spec.packer_format_version != 3:
                    pytest.fail("producer semantic version must be 3")
    if descriptors[0] != descriptors[1]:
        pytest.fail("packers disagree on wire descriptor")


@pytest.mark.parametrize("version", [2, 4])
def test_pack_rejects_wrong_producer_version_before_output(tmp_path, version):
    from dataclasses import replace

    _write_fixture(tmp_path, ["42000001"])
    out = tmp_path / "out"
    with ShardPackSession("2026-08", ShardGeometry(2), parquets_base=str(tmp_path)) as session:
        current = session.materialization_spec("42", input_sha1s={})
        wrong = replace(current, packer_format_version=version)
        with pytest.raises(ValueError, match="packer_format_version"):
            session.pack("42", out, materialization=wrong)
    if out.exists():
        pytest.fail("wrong producer version wrote output")
