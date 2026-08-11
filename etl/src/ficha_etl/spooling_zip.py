"""ZIP writer with a disk-backed central directory.

``zipfile.ZipFile`` keeps one ``ZipInfo`` in both ``filelist`` and
``NameToInfo`` until close. That is fine for ordinary archives, but
``companies.zip`` has one member per CNPJ base (~68M at current scale), so the
metadata cache alone would exceed the Actions runner RAM.

``SpoolingZipFile`` keeps the stdlib local-header/compression implementation,
but serializes each central-directory record to a temporary file as soon as
the member closes. Closing the archive streams that spool into the ZIP and
writes the standard ZIP64 trailer. RAM is therefore independent of member
count.

This is intentionally write-only. The caller must guarantee unique member
names; ``pack_companies`` already does so with its strictly-increasing
``cnpj_base`` guard, while schema/lookup/meta member names are fixed and
distinct.
"""

from __future__ import annotations

import shutil
import struct
import tempfile
import zipfile
from pathlib import Path

_COPY_CHUNK = 8 * 1024 * 1024


class _DiscardNameIndex:
    """Write-only replacement for ZipFile.NameToInfo without O(N) retention."""

    def __contains__(self, key: object) -> bool:
        return False

    def __setitem__(self, key: object, value: object) -> None:
        return None


class _CentralDirectorySpool:
    def __init__(self, *, directory: Path | None = None) -> None:
        self._fp = tempfile.TemporaryFile(dir=directory)
        self._count = 0

    def __len__(self) -> int:
        return self._count

    @staticmethod
    def _strip_zip64_extra(extra: bytes) -> bytes:
        """Remove an existing ZIP64 (0x0001) extra field, preserving others."""
        out = bytearray()
        pos = 0
        while pos + 4 <= len(extra):
            field_id, size = struct.unpack_from("<HH", extra, pos)
            end = pos + 4 + size
            if end > len(extra):
                out.extend(extra[pos:])
                return bytes(out)
            if field_id != 0x0001:
                out.extend(extra[pos:end])
            pos = end
        out.extend(extra[pos:])
        return bytes(out)

    def append(self, zinfo: zipfile.ZipInfo) -> None:
        """Serialize one already-written member's central-directory record."""
        dt = zinfo.date_time
        dosdate = (dt[0] - 1980) << 9 | dt[1] << 5 | dt[2]
        dostime = dt[3] << 11 | dt[4] << 5 | (dt[5] // 2)

        zip64_values: list[int] = []
        if zinfo.file_size > zipfile.ZIP64_LIMIT or zinfo.compress_size > zipfile.ZIP64_LIMIT:
            zip64_values.extend((zinfo.file_size, zinfo.compress_size))
            file_size = compress_size = 0xFFFFFFFF
        else:
            file_size = zinfo.file_size
            compress_size = zinfo.compress_size

        if zinfo.header_offset > zipfile.ZIP64_LIMIT:
            zip64_values.append(zinfo.header_offset)
            header_offset = 0xFFFFFFFF
        else:
            header_offset = zinfo.header_offset

        extra_data = zinfo.extra
        min_version = 0
        if zip64_values:
            extra_data = self._strip_zip64_extra(extra_data)
            extra_data = (
                struct.pack(
                    "<HH" + "Q" * len(zip64_values),
                    0x0001,
                    8 * len(zip64_values),
                    *zip64_values,
                )
                + extra_data
            )
            min_version = zipfile.ZIP64_VERSION

        if zinfo.compress_type == zipfile.ZIP_BZIP2:
            min_version = max(zipfile.BZIP2_VERSION, min_version)
        elif zinfo.compress_type == zipfile.ZIP_LZMA:
            min_version = max(zipfile.LZMA_VERSION, min_version)

        extract_version = max(min_version, zinfo.extract_version)
        create_version = max(min_version, zinfo.create_version)
        filename, flag_bits = zinfo._encodeFilenameFlags()  # noqa: SLF001
        header = struct.pack(
            zipfile.structCentralDir,
            zipfile.stringCentralDir,
            create_version,
            zinfo.create_system,
            extract_version,
            zinfo.reserved,
            flag_bits,
            zinfo.compress_type,
            dostime,
            dosdate,
            zinfo.CRC,
            compress_size,
            file_size,
            len(filename),
            len(extra_data),
            len(zinfo.comment),
            0,
            zinfo.internal_attr,
            zinfo.external_attr,
            header_offset,
        )
        self._fp.write(header)
        self._fp.write(filename)
        self._fp.write(extra_data)
        self._fp.write(zinfo.comment)
        self._count += 1

    def copy_to(self, fp) -> None:
        self._fp.flush()
        self._fp.seek(0)
        shutil.copyfileobj(self._fp, fp, length=_COPY_CHUNK)

    def close(self) -> None:
        self._fp.close()


class SpoolingZipFile(zipfile.ZipFile):
    """Write-only ``ZipFile`` whose central directory is disk-backed."""

    def __init__(self, file, mode: str = "w", *args, **kwargs) -> None:
        if mode not in ("w", "x"):
            raise ValueError("SpoolingZipFile supports only write/create modes")
        super().__init__(file, mode, *args, **kwargs)
        directory = Path(file).resolve().parent if isinstance(file, (str, Path)) else None
        self._central_spool = _CentralDirectorySpool(directory=directory)
        self.filelist = self._central_spool
        self.NameToInfo = _DiscardNameIndex()

    def _write_end_record(self) -> None:
        """Stream the spooled directory, then emit stdlib-compatible ZIP64 EOCD."""
        cent_dir_offset = self.start_dir
        self._central_spool.copy_to(self.fp)
        zip64_record_offset = self.fp.tell()
        cent_dir_count = len(self._central_spool)
        cent_dir_size = zip64_record_offset - cent_dir_offset

        requires_zip64 = (
            cent_dir_count > zipfile.ZIP_FILECOUNT_LIMIT
            or cent_dir_offset > zipfile.ZIP64_LIMIT
            or cent_dir_size > zipfile.ZIP64_LIMIT
        )
        if requires_zip64:
            if not self._allowZip64:
                raise zipfile.LargeZipFile("archive would require ZIP64 extensions")
            self.fp.write(
                struct.pack(
                    zipfile.structEndArchive64,
                    zipfile.stringEndArchive64,
                    44,
                    45,
                    45,
                    0,
                    0,
                    cent_dir_count,
                    cent_dir_count,
                    cent_dir_size,
                    cent_dir_offset,
                )
            )
            self.fp.write(
                struct.pack(
                    zipfile.structEndArchive64Locator,
                    zipfile.stringEndArchive64Locator,
                    0,
                    zip64_record_offset,
                    1,
                )
            )
            eocd_count = min(cent_dir_count, 0xFFFF)
            eocd_size = min(cent_dir_size, 0xFFFFFFFF)
            eocd_offset = min(cent_dir_offset, 0xFFFFFFFF)
        else:
            eocd_count = cent_dir_count
            eocd_size = cent_dir_size
            eocd_offset = cent_dir_offset

        self.fp.write(
            struct.pack(
                zipfile.structEndArchive,
                zipfile.stringEndArchive,
                0,
                0,
                eocd_count,
                eocd_count,
                eocd_size,
                eocd_offset,
                len(self._comment),
            )
        )
        self.fp.write(self._comment)
        self.fp.flush()

    def close(self) -> None:
        spool = getattr(self, "_central_spool", None)
        try:
            super().close()
        finally:
            if spool is not None:
                spool.close()
                self._central_spool = None
