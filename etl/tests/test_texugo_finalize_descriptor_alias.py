"""P0-A negative control: finalization must not clobber its input descriptor.

Tests only path validation before descriptor/network I/O. The four adversarial
cases are known XFAIL until the owning #246 implementation rejects aliases.
No ETL, Internet Archive access or publication occurs in these tests.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from ficha_etl import monthly_sharded


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="P0-A #246: output aliases of the input descriptor are not rejected yet",
)
@pytest.mark.parametrize(
    ("output_name", "alias_kind"),
    [
        ("snapshot_entry", "same_path"),
        ("verification", "same_path"),
        ("snapshot_entry", "symlink"),
        ("verification", "hardlink"),
    ],
)
def test_finalize_rejects_descriptor_output_alias_before_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    output_name: str,
    alias_kind: str,
) -> None:
    descriptor = tmp_path / "production-descriptor.json"
    original = b'{"date":"2026-08","source":"pinned"}\n'
    descriptor.write_bytes(original)

    args = argparse.Namespace(
        month="2026-08",
        descriptor=descriptor,
        snapshot_entry=tmp_path / "snapshot-entry.json",
        verification=tmp_path / "verification.json",
        manifest=tmp_path / "manifest.json",
    )

    if alias_kind == "same_path":
        setattr(args, output_name, descriptor)
    elif alias_kind == "symlink":
        getattr(args, output_name).symlink_to(descriptor)
    else:
        getattr(args, output_name).hardlink_to(descriptor)

    def forbidden_descriptor_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("descriptor read before input/output alias validation")

    monkeypatch.setattr(monthly_sharded, "_load_descriptor", forbidden_descriptor_read)

    with pytest.raises(ValueError, match="descriptor"):
        monthly_sharded.command_finalize(args)

    assert descriptor.read_bytes() == original
    assert not args.manifest.exists()
