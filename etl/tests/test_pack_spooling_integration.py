from __future__ import annotations

import zipfile

from ficha_etl import pack
from ficha_etl.spooling_zip import SpoolingZipFile


def test_pack_companies_uses_spooling_zip(monkeypatch, tmp_path):
    used = False

    class TrackingZip(SpoolingZipFile):
        def __init__(self, *args, **kwargs):
            nonlocal used
            used = True
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(pack, "SpoolingZipFile", TrackingZip)
    output = tmp_path / "companies.zip"
    lookup_rows = {kind: [] for kind in pack.LOOKUP_KINDS}

    result = pack.pack_companies(iter(()), lookup_rows, output, "2026-05")

    assert used is True
    assert result["count"] == 0
    assert result["size_bytes"] == output.stat().st_size
    with zipfile.ZipFile(output) as zf:
        assert zf.testzip() is None
        assert "_schema.desc" in zf.namelist()
        assert "_meta.json" in zf.namelist()
        assert all(f"_lookups/{kind}.pb" in zf.namelist() for kind in pack.LOOKUP_KINDS)
