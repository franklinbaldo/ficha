"""P1 #281: executable, test-only contract for RFB source/output equivalence.

All ten ZIPs and their source-side receipts are generated from tiny synthetic
CSV rows. This is an offline oracle, NOT an implementation or proof of the
monthly producer's publication gate, and NOT a real RFB provenance check.
The Python Counter is fixture-sized: production requires external sorting or
bucketed comparison under an explicit memory/disk budget.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path

import pytest


MONTH = "2026-08"
PART_NAMES = tuple(f"Estabelecimentos{i}.zip" for i in range(10))
KEY_RE = re.compile(r"[0-9A-Z]{12}[0-9]{2}\Z")


@dataclass(frozen=True)
class SourceReceipt:
    """Witness collected from *source ZIPs*, never from produced keys."""

    month: str
    name: str
    zip_sha256: str
    csv_member: str
    member_sha256: str
    key_multiset_sha256: str
    rows: int


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _source_keys(zip_path: Path) -> tuple[list[str], bytes, str]:
    """Mini-fixture ZIP reader; deliberately not production's DuckDB parser."""
    if not zip_path.is_file():
        raise ValueError("missing source ZIP")
    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = [item for item in archive.infolist() if not item.is_dir()]
            if len(members) != 1 or len(archive.infolist()) != 1:
                raise ValueError("expected one CSV member")
            member = members[0]
            if (
                not member.filename.endswith(".ESTABELE")
                or "/" in member.filename
                or "\\" in member.filename
            ):
                raise ValueError("unexpected ZIP member")
            raw = archive.read(member)  # zipfile validates CRC for this fixture
    except (zipfile.BadZipFile, RuntimeError) as exc:
        raise ValueError("corrupt source ZIP") from exc
    try:
        text = raw.decode("latin-1")
        parsed = list(csv.reader(io.StringIO(text, newline=""), delimiter=";", quotechar='"'))
    except (UnicodeError, csv.Error) as exc:
        raise ValueError("malformed source CSV") from exc
    if not parsed:
        raise ValueError("empty source part")
    keys: list[str] = []
    for row in parsed:
        if len(row) != 30:
            raise ValueError("unexpected estabelecimento CSV layout")
        key = "".join(row[:3])  # RFB: basico VARCHAR8 + ordem VARCHAR4 + DV VARCHAR2
        if not KEY_RE.fullmatch(key) or any(len(f) != n for f, n in zip(row[:3], (8, 4, 2))):
            raise ValueError("invalid or non-textual source key")
        keys.append(key)
    return keys, raw, member.filename


def _capture(month: str, name: str, path: Path) -> SourceReceipt:
    keys, csv_raw, member = _source_keys(path)
    return SourceReceipt(
        month=month,
        name=name,
        zip_sha256=_digest(path.read_bytes()),
        csv_member=member,
        member_sha256=_digest(csv_raw),
        key_multiset_sha256=_digest("".join(sorted(keys)).encode("ascii")),
        rows=len(keys),
    )


def _reconcile(
    month: str,
    source_zips: dict[str, Path],
    receipts: tuple[SourceReceipt, ...],
    produced_keys: list[str],
) -> None:
    """Offline reference contract: independent source witness + exact multiset."""
    if set(source_zips) != set(PART_NAMES) or len(source_zips) != len(PART_NAMES):
        raise ValueError("missing or unexpected source part")
    if len(receipts) != len(PART_NAMES) or {r.name for r in receipts} != set(PART_NAMES):
        raise ValueError("missing or duplicate receipt")
    if any(r.month != month for r in receipts):
        raise ValueError("mixed source competence")
    if len({str(p.resolve()) for p in source_zips.values()}) != 10:
        raise ValueError("aliased source paths")
    by_name = {r.name: r for r in receipts}
    keys = Counter()
    zip_hashes = set()
    for name in PART_NAMES:
        path = source_zips[name]
        actual = _capture(month, name, path)
        if actual != by_name[name]:
            raise ValueError(f"source receipt mismatch: {name}")
        if actual.zip_sha256 in zip_hashes:
            raise ValueError("duplicate ZIP byte identity across parts")
        zip_hashes.add(actual.zip_sha256)
        source_part_keys, _, _ = _source_keys(path)
        keys.update(source_part_keys)
    if not keys or any(n != 1 for n in keys.values()):
        raise ValueError("empty or duplicate source identity")
    if keys != Counter(produced_keys):
        raise ValueError("source/output key multiset mismatch")


def _dv(base: str, weights: tuple[int, ...]) -> str:
    remainder = sum((ord(char) - 48) * weight for char, weight in zip(base, weights)) % 11
    return str(0 if remainder < 2 else 11 - remainder)


def _key(base12: str) -> str:
    a = _dv(base12, (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    b = _dv(base12 + a, (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    return base12 + a + b


def _write_zip(path: Path, keys: list[str], *, extra_member: bool = False) -> None:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
    for key in keys:
        writer.writerow([key[:8], key[8:12], key[12:], *([""] * 27)])
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("K.synthetic.ESTABELE", stream.getvalue().encode("latin-1"))
        if extra_member:
            archive.writestr("unexpected.txt", "extra")


def _fixture(root: Path, *, rows: dict[int, list[str]] | None = None):
    root.mkdir(parents=True, exist_ok=True)
    keys = [_key("00000000E08G") if i == 0 else _key(f"{i:08d}0001") for i in range(10)]
    assert keys[0] == "00000000E08G12"
    zips: dict[str, Path] = {}
    receipts: list[SourceReceipt] = []
    expected: list[str] = []
    for i, name in enumerate(PART_NAMES):
        part_keys = rows[i] if rows is not None and i in rows else [keys[i]]
        path = root / name
        _write_zip(path, part_keys)
        zips[name] = path
        if part_keys:
            receipts.append(_capture(MONTH, name, path))
        expected.extend(part_keys)
    return zips, tuple(receipts), expected


def test_ten_zips_alpha_key_and_output_permutation(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    assert len(receipts) == 10
    assert receipts[0].zip_sha256 != receipts[0].member_sha256
    _reconcile(MONTH, zips, receipts, list(reversed(keys)))


def test_source_row_order_is_not_material(tmp_path: Path) -> None:
    keys = [_key("00000000E08G"), _key("888888880001")]
    zips, receipts, output = _fixture(tmp_path, rows={0: keys})
    _write_zip(zips[PART_NAMES[0]], list(reversed(keys)))
    changed = _capture(MONTH, PART_NAMES[0], zips[PART_NAMES[0]])
    assert changed.key_multiset_sha256 == receipts[0].key_multiset_sha256
    assert changed.member_sha256 != receipts[0].member_sha256
    # A trusted byte receipt is still necessary, even with identical semantic keys.
    with pytest.raises(ValueError, match="source receipt mismatch"):
        _reconcile(MONTH, zips, receipts, output)
    repaired = (changed, *receipts[1:])
    _reconcile(MONTH, zips, repaired, output)


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_missing_or_extra_source_part(tmp_path: Path, mutation: str) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    if mutation == "missing":
        zips.pop(PART_NAMES[9])
    else:
        zips["Estabelecimentos10.zip"] = zips[PART_NAMES[9]]
    with pytest.raises(ValueError, match="missing or unexpected source part"):
        _reconcile(MONTH, zips, receipts, keys)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "month"])
def test_receipt_integrity_and_competence(tmp_path: Path, mutation: str) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    if mutation == "missing":
        receipts = receipts[:-1]
    elif mutation == "duplicate":
        receipts = (*receipts[:-1], receipts[0])
    else:
        receipts = (replace(receipts[0], month="2026-09"), *receipts[1:])
    with pytest.raises(ValueError, match="receipt|competence"):
        _reconcile(MONTH, zips, receipts, keys)


def test_zero_chunk_empty_source_part(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    _write_zip(zips[PART_NAMES[4]], [])
    with pytest.raises(ValueError, match="empty source part"):
        _reconcile(MONTH, zips, receipts, keys)


def test_all_sources_empty_fail_not_vacuously_pass(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path, rows={i: [] for i in range(10)})
    with pytest.raises(ValueError, match="missing or duplicate receipt"):
        _reconcile(MONTH, zips, receipts, keys)


@pytest.mark.parametrize("mutation", ["truncate", "replace"])
def test_truncation_and_equal_cardinality_substitution(tmp_path: Path, mutation: str) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    produced = keys[:-1] if mutation == "truncate" else [*keys[:-1], _key("999999990001")]
    if mutation == "replace":
        assert len(produced) == len(keys) and len(set(produced)) == len(keys)
    with pytest.raises(ValueError, match="source/output key multiset mismatch"):
        _reconcile(MONTH, zips, receipts, produced)


def test_source_zip_tampered_after_capture(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    _write_zip(zips[PART_NAMES[2]], [_key("999999990001")])
    with pytest.raises(ValueError, match="source receipt mismatch"):
        _reconcile(MONTH, zips, receipts, keys)


def test_receipt_tampered_digest_or_count(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    for changes in ({"zip_sha256": "0" * 64}, {"rows": 2}, {"member_sha256": "0" * 64}):
        forged = (replace(receipts[0], **changes), *receipts[1:])
        with pytest.raises(ValueError, match="source receipt mismatch"):
            _reconcile(MONTH, zips, forged, keys)


def test_duplicate_zip_bytes_relabelled_as_another_part(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    zips[PART_NAMES[1]].write_bytes(zips[PART_NAMES[0]].read_bytes())
    new_receipt = _capture(MONTH, PART_NAMES[1], zips[PART_NAMES[1]])
    receipts = (receipts[0], new_receipt, *receipts[2:])
    with pytest.raises(ValueError, match="duplicate ZIP byte identity"):
        _reconcile(MONTH, zips, receipts, keys)


def test_alias_same_path_rejected(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    zips[PART_NAMES[3]] = zips[PART_NAMES[2]]
    with pytest.raises(ValueError, match="aliased source paths"):
        _reconcile(MONTH, zips, receipts, keys)


def test_duplicate_full_key_across_parts_rejected(tmp_path: Path) -> None:
    keys = [_key("00000000E08G")]
    zips, receipts, output = _fixture(tmp_path, rows={0: keys * 2})
    with pytest.raises(ValueError, match="empty or duplicate source identity"):
        _reconcile(MONTH, zips, receipts, output)


def test_extra_zip_member_rejected(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    _write_zip(zips[PART_NAMES[4]], [keys[4]], extra_member=True)
    with pytest.raises(ValueError, match="expected one CSV member"):
        _reconcile(MONTH, zips, receipts, keys)


def test_corrupted_zip_rejected(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    zips[PART_NAMES[5]].write_bytes(b"not a zip")
    with pytest.raises(ValueError, match="corrupt source ZIP"):
        _reconcile(MONTH, zips, receipts, keys)


def test_zip_repack_same_csv_changes_zip_identity(tmp_path: Path) -> None:
    zips, receipts, keys = _fixture(tmp_path)
    path = zips[PART_NAMES[0]]
    with zipfile.ZipFile(path) as archive:
        csv_bytes = archive.read("K.synthetic.ESTABELE")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("K.synthetic.ESTABELE", csv_bytes)
    updated = _capture(MONTH, PART_NAMES[0], path)
    assert updated.member_sha256 == receipts[0].member_sha256
    assert updated.key_multiset_sha256 == receipts[0].key_multiset_sha256
    assert updated.zip_sha256 != receipts[0].zip_sha256
    with pytest.raises(ValueError, match="source receipt mismatch"):
        _reconcile(MONTH, zips, receipts, keys)
