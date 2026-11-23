"""Offline-only regression for the dormant legacy raw-cache upload helper.

A callable code path is not evidence of a production invocation.
The sentinel replaces the IA client: no network access is made.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from ficha_etl import upload
from ficha_etl.sources import canonical_inventory


class LegacyRawEgressReached(AssertionError):
    """Historical cache helper reached the raw IA write boundary."""


@pytest.mark.xfail(
    strict=True,
    raises=LegacyRawEgressReached,
    reason="#433/F15: legacy raw-cache helper lacks global custody admission",
)
def test_legacy_cache_helper_refuses_partial_raw_without_any_ia_egress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """One well-formed ZIP out of 37 cannot authorize any raw write."""
    specs = list(canonical_inventory())
    assert len(specs) == 37
    assert len({spec.name for spec in specs}) == 37
    source = tmp_path / "2026-07" / specs[0].name
    source.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("source.csv", "cnpj;value\n1;A\n")

    def reject_sink(_identifier: str, **kwargs: object) -> None:
        files = kwargs.get("files")
        assert isinstance(files, dict)
        assert list(files) == [f"raw/{specs[0].name}"]
        raise LegacyRawEgressReached(
            "legacy helper reached ia.upload with just 1/37 raw ZIPs"
        )

    monkeypatch.setattr(upload.ia, "upload", reject_sink)

    # A future fail-closed guard can refuse the partial lot by raising.
    # No-op silence and unguarded IA calls are not accepted as passing.
    with pytest.raises((RuntimeError, ValueError, FileNotFoundError)):
        upload.upload_raw_zips(
            "2026-07", tmp_path, access_key="fake", secret_key="fake"
        )
