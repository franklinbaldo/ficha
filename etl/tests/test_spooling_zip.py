from __future__ import annotations

import zipfile

import pytest

from ficha_etl.spooling_zip import SpoolingZipFile


def test_spooling_zip_is_readable_by_stdlib(tmp_path):
    path = tmp_path / "companies.zip"
    payloads = {
        "_schema.desc": b"schema",
        "_lookups/cnaes.pb": b"lookup",
        "00/000/001.pb": b"company-1",
        "99/999/999.pb": b"company-99999999",
        "_meta.json": b'{"count": 2}',
    }

    with SpoolingZipFile(
        path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
        allowZip64=True,
    ) as zf:
        for name, payload in payloads.items():
            zf.writestr(name, payload)
        assert not isinstance(zf.filelist, list)
        assert not isinstance(zf.NameToInfo, dict)

    with zipfile.ZipFile(path) as zf:
        assert zf.testzip() is None
        assert set(zf.namelist()) == set(payloads)
        for name, payload in payloads.items():
            assert zf.read(name) == payload


def test_spooling_zip_writes_zip64_for_member_count(tmp_path):
    path = tmp_path / "many.zip"
    count = zipfile.ZIP_FILECOUNT_LIMIT + 2

    with SpoolingZipFile(path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
        for i in range(count):
            zf.writestr(f"00/{i // 1000:03d}/{i % 1000:03d}.pb", b"x")

    with zipfile.ZipFile(path) as zf:
        assert len(zf.infolist()) == count
        assert zf.read("00/000/000.pb") == b"x"
        last = count - 1
        assert zf.read(f"00/{last // 1000:03d}/{last % 1000:03d}.pb") == b"x"


def test_spooling_zip_rejects_read_and_append_modes(tmp_path):
    path = tmp_path / "x.zip"
    with pytest.raises(ValueError, match="write/create"):
        SpoolingZipFile(path, "r")
    with pytest.raises(ValueError, match="write/create"):
        SpoolingZipFile(path, "a")
