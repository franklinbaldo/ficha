from __future__ import annotations

from dataclasses import dataclass

import pytest

from ficha_etl.materialization import MaterializationSpec, ShardRange
from ficha_etl.shard_remote import (
    MaterializationInputsUnavailable,
    SHARD_INPUT_NAMES,
    ShardReuseState,
    classify_remote_shard,
    fetch_remote_shard_meta,
    materialization_input_sha1s,
)


def _spec(prefix: str = "07") -> MaterializationSpec:
    return MaterializationSpec(
        snapshot="2026-05",
        shard_range=ShardRange(prefix),
        inputs={name: f"{index:040x}" for index, name in enumerate(SHARD_INPUT_NAMES, start=1)},
        descriptor_sha256="d" * 64,
    )


def _entry(name: str, *, size: object = "123", sha1: object = "a" * 40) -> dict:
    return {"name": name, "size": size, "sha1": sha1}


def _metadata(*entries: dict, pending_tasks: object = None) -> dict:
    return {"files": list(entries), "pending_tasks": pending_tasks}


def test_materialization_inputs_cover_parquets_and_embedded_lookups():
    metadata = _metadata(
        *[
            _entry(name, size=str(index), sha1=f"{index:040x}")
            for index, name in enumerate(SHARD_INPUT_NAMES, start=1)
        ]
    )
    result = materialization_input_sha1s(metadata)

    assert tuple(result) == SHARD_INPUT_NAMES
    assert set(result) == {
        "cnpjs.parquet",
        "raizes.parquet",
        "socios.parquet",
        "lookups/cnaes.parquet",
        "lookups/motivos.parquet",
        "lookups/municipios.parquet",
        "lookups/naturezas.parquet",
        "lookups/paises.parquet",
        "lookups/qualificacoes.parquet",
    }


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {},
        {"files": None},
        _metadata(*[_entry(name) for name in SHARD_INPUT_NAMES[:-1]]),
        _metadata(
            *[
                _entry(name, size=0 if index == 0 else 1)
                for index, name in enumerate(SHARD_INPUT_NAMES)
            ]
        ),
    ],
)
def test_materialization_inputs_fail_closed(metadata):
    with pytest.raises(MaterializationInputsUnavailable):
        materialization_input_sha1s(metadata)


def test_absence_requires_a_valid_files_observation():
    expected = _spec()
    unknown = classify_remote_shard("07", expected, None, fetch_meta=lambda _: None)
    absent = classify_remote_shard("07", expected, _metadata(), fetch_meta=lambda _: None)

    assert unknown.state is ShardReuseState.UNKNOWN
    assert absent.state is ShardReuseState.ABSENT
    assert not unknown.may_skip
    assert not absent.may_skip


def test_present_shard_needs_comparable_file_identity_before_meta():
    expected = _spec()
    verdict = classify_remote_shard(
        "07",
        expected,
        _metadata(_entry("companies-07.zip", sha1=None)),
        fetch_meta=lambda _: pytest.fail("não deve ler membro sem identidade do objeto"),
    )
    assert verdict.state is ShardReuseState.UNKNOWN


def test_exact_materialization_is_reusable_even_with_pending_tasks_true():
    expected = _spec()
    payload = {
        "materialization": {
            "id": expected.materialization_id(),
            "spec": expected.as_document(),
        }
    }
    verdict = classify_remote_shard(
        "07",
        expected,
        _metadata(_entry("companies-07.zip"), pending_tasks=True),
        fetch_meta=lambda _: payload,
    )

    assert verdict.state is ShardReuseState.REUSABLE
    assert verdict.may_skip
    assert verdict.size == 123
    assert verdict.sha1 == "a" * 40


def test_same_name_with_different_materialization_is_mismatch():
    expected = _spec()
    other = _spec("08")
    verdict = classify_remote_shard(
        "07",
        expected,
        _metadata(_entry("companies-07.zip")),
        fetch_meta=lambda _: {
            "materialization": {
                "id": other.materialization_id(),
                "spec": other.as_document(),
            }
        },
    )
    assert verdict.state is ShardReuseState.MISMATCH
    assert not verdict.may_skip


@pytest.mark.parametrize("payload", [None, [], {}, {"materialization": None}])
def test_unreadable_or_incomplete_meta_is_unknown(payload):
    verdict = classify_remote_shard(
        "07",
        _spec(),
        _metadata(_entry("companies-07.zip")),
        fetch_meta=lambda _: payload,
    )
    assert verdict.state is ShardReuseState.UNKNOWN


@dataclass
class _Response:
    status_code: int
    payload: object = None

    def json(self):
        return self.payload


class _Client:
    def __init__(self, responses: list[_Response]):
        self.responses = responses
        self.urls: list[str] = []

    def get(self, url: str):
        self.urls.append(url)
        return self.responses.pop(0)


def test_transparent_unzip_retries_503_without_waiting_for_pending_tasks():
    client = _Client([_Response(503), _Response(503), _Response(200, {"ok": True})])
    sleeps: list[float] = []

    payload = fetch_remote_shard_meta(
        "2026-05",
        "companies-07.zip",
        attempts=5,
        backoff_s=10,
        sleep=sleeps.append,
        client=client,
    )

    assert payload == {"ok": True}
    assert sleeps == [10, 20]
    assert all(url.endswith("/ficha-2026-05/companies-07.zip/_meta.json") for url in client.urls)
