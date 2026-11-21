"""Freeze standalone companies.zip before handing bytes to the Internet Archive SDK.

The freeze protects a pack output from later pathname mutation during PUT.
It does not authenticate independently the inputs read by the packer.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import tempfile
from contextlib import ExitStack
from pathlib import Path
from typing import BinaryIO

_CHUNK = 1024 * 1024


def freeze_pack_snapshot(
    stack: ExitStack, path: Path, expected_size: int, *, expected_sha256: str
) -> BinaryIO:
    """Verify and freeze the pack output in an anonymous handle.

    Re-reading the opened inode detects same-size rewrites during the copy.
    Any local error must return before the first PUT. The caller keeps stack
    open until the synchronous upload completes.
    """
    if type(expected_size) is not int or expected_size <= 0:
        raise ValueError("pack returned invalid ZIP size")
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("pack returned invalid ZIP SHA-256")

    frozen = stack.enter_context(tempfile.TemporaryFile(mode="w+b"))
    if hasattr(os, "fstatvfs"):
        vfs = os.fstatvfs(frozen.fileno())
        available = vfs.f_bavail * vfs.f_frsize
        margin = max(64 * 1024 * 1024, (expected_size + 9) // 10)
        if available < expected_size + margin or vfs.f_favail < 1:
            raise OSError(
                "TMPDIR insufficient for pack snapshot: "
                f"need={expected_size + margin}, available={available}, "
                f"free_inodes={vfs.f_favail}"
            )

    with path.open("rb") as original:
        before = os.fstat(original.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size != expected_size:
            raise ValueError("companies.zip differs from pack output size or is not a regular file")

        copied = 0
        digest = hashlib.sha256()
        for chunk in iter(lambda: original.read(_CHUNK), b""):
            copied += len(chunk)
            if copied > expected_size:
                raise ValueError("companies.zip grew during snapshot")
            frozen.write(chunk)
            digest.update(chunk)
        if copied != expected_size:
            raise ValueError("companies.zip truncated during snapshot")
        if digest.hexdigest() != expected_sha256:
            raise ValueError("companies.zip differs from producer SHA-256")

        # Check the same inode again, not a path that another process can replace.
        original.seek(0)
        checked = 0
        source_hash = hashlib.sha256()
        for chunk in iter(lambda: original.read(_CHUNK), b""):
            checked += len(chunk)
            source_hash.update(chunk)
        after = os.fstat(original.fileno())
        if checked != expected_size or source_hash.digest() != digest.digest():
            raise ValueError("companies.zip changed while freezing")
        current = path.stat()
        if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
            raise ValueError("companies.zip path replaced while freezing")
        if (
            after.st_size != before.st_size
            or after.st_mtime_ns != before.st_mtime_ns
            or after.st_ctime_ns != before.st_ctime_ns
        ):
            raise ValueError("companies.zip metadata changed while freezing")

    frozen.seek(0)
    frozen_hash = hashlib.sha256()
    frozen_size = 0
    for chunk in iter(lambda: frozen.read(_CHUNK), b""):
        frozen_hash.update(chunk)
        frozen_size += len(chunk)
    if frozen_size != expected_size or frozen_hash.digest() != digest.digest():
        raise ValueError("companies.zip anonymous snapshot did not match source")
    frozen.seek(0)
    return frozen
