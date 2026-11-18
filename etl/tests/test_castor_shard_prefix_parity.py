"""Exhaustive P0-A partition proof shared with the TypeScript reader.

Canonical transcript: alphabet ordered 0..9,A..Z; each line PREFIX:BUCKET\n.
The digest is fixed independently of either implementation.
"""

from __future__ import annotations

import hashlib
from itertools import product

from ficha_etl.cnpj import shard_of, source_prefixes

_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_DIGEST = "3f9eed1068515339d6528137d19acac79c4259ed3341b1b8b83f6b05821aca6e"


def test_all_1296_prefixes_have_one_stable_bucket_and_js_parity_digest() -> None:
    prefixes = ["".join(pair) for pair in product(_ALPHABET, repeat=2)]
    assert len(prefixes) == 1296
    actual: dict[str, str] = {}
    for prefix in prefixes:
        bucket = shard_of(prefix + "000000")
        expected = f"{(int(prefix) if prefix.isdigit() else int(prefix, 36) % 100):02d}"
        assert bucket == expected, prefix
        if prefix.isdigit():
            assert bucket == prefix  # v1 numeric snapshot bucket is unchanged
        actual[prefix] = bucket

    transcript = "".join(f"{prefix}:{actual[prefix]}\n" for prefix in prefixes)
    assert hashlib.sha256(transcript.encode("ascii")).hexdigest() == _DIGEST
    assert set(actual.values()) == {f"{n:02d}" for n in range(100)}

    inverse: list[str] = []
    for bucket in sorted(set(actual.values())):
        members = source_prefixes(bucket)
        assert len(members) == len(set(members))
        assert all(actual[prefix] == bucket for prefix in members)
        inverse.extend(members)
    assert sorted(inverse) == prefixes


def test_invalid_roots_never_select_a_bucket() -> None:
    import pytest

    for value in ("aa000000", "ZZ00000", "ZZ0000000", "AB!00000", True, -1):
        with pytest.raises((ValueError, TypeError)):
            shard_of(value)
