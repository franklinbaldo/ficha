"""P0-A: legacy upload namespace cannot receive wire v1.1 through any API.

Small, offline, negative-first tests. No network, IA, DuckDB queries or PUT.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ficha_etl.shard_publish import ShardPublishError, publish_one_shard
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY
from ficha_etl.shard_transfer import PreparedShard, submit_one_shard, submit_prepared_shard
from ficha_etl.sharded_pack import ShardPackSession


class ReachedLocalSpec(Exception):
    """The legacy contract passed and local materialization began."""


class _ProbeSession:
    geometry = PUBLIC_COMPANIES_GEOMETRY
    month = "2026-08"

    def __init__(self, wire: str | None, mapping: str | None):
        self.wire_version = wire
        self.shard_by = mapping

    def materialization_spec(self, *_args: object, **_kwargs: object) -> None:
        raise ReachedLocalSpec


def _forbid_io(*_args: object, **_kwargs: object) -> None:
    pytest.fail("legacy upload guard must run before remote or local IO")


def _invoke(entrypoint: str, session: object, tmp_path: Path) -> None:
    kwargs = {
        "pinned_inputs": {},
        "fetch_metadata": _forbid_io,
        "fetch_meta": _forbid_io,
        "upload": _forbid_io,
        "fetch_direct": _forbid_io,
    }
    if entrypoint == "submit-one":
        submit_one_shard(session, "00", tmp_path, **kwargs)
    elif entrypoint == "submit-prepared":
        prepared = PreparedShard(
            prefix="00",
            name="companies-00.zip",
            path=tmp_path / "never-read.zip",
            materialization_id="never-read",
            size=0,
            sha1="0" * 40,
        )
        submit_prepared_shard(session, prepared, **kwargs)
    elif entrypoint == "publish-one":
        publish_one_shard(session, "00", tmp_path, **kwargs)
    else:
        raise AssertionError(entrypoint)


_ENTRYPOINTS = ("submit-one", "submit-prepared", "publish-one")


@pytest.mark.parametrize("entrypoint", _ENTRYPOINTS)
@pytest.mark.parametrize(
    ("wire", "mapping"),
    [
        ("1.1.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        ("1.1.0", "cnpj_base_prefix_2"),
        ("1.0.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        (None, None),
    ],
)
def test_unsafe_or_unknown_contract_rejected_before_any_io(
    tmp_path: Path, entrypoint: str, wire: str | None, mapping: str | None
) -> None:
    with pytest.raises(ShardPublishError, match="unversioned companies-NN.zip"):
        _invoke(entrypoint, _ProbeSession(wire, mapping), tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("entrypoint", _ENTRYPOINTS)
def test_current_real_producer_rejected_by_every_upload_entrypoint(
    tmp_path: Path, entrypoint: str
) -> None:
    session = ShardPackSession("2026-08", PUBLIC_COMPANIES_GEOMETRY)
    assert (session.wire_version, session.shard_by) == (
        "1.1.0",
        "cnpj_base_prefix_or_base36_mod_100_v1",
    )
    with pytest.raises(ShardPublishError, match="unversioned companies-NN.zip"):
        _invoke(entrypoint, session, tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("entrypoint", _ENTRYPOINTS)
def test_v1_contract_reaches_local_materialization_but_not_remote_io(
    tmp_path: Path, entrypoint: str
) -> None:
    with pytest.raises(ReachedLocalSpec):
        _invoke(entrypoint, _ProbeSession("1.0.0", "cnpj_base_prefix_2"), tmp_path)
    assert list(tmp_path.iterdir()) == []
