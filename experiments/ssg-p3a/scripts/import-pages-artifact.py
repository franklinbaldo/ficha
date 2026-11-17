#!/usr/bin/env python3
"""Safely extract an existing GitHub Pages artifact into a new offline directory.

Strictly read-only with respect to GitHub/Pages and the source archive. Does not
retrieve artifacts or publish anything. Expected digests must be supplied out
of band, never inferred from the archive being accepted.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import tempfile
import zipfile

MAX_BYTES = 32 * 1024 * 1024
HEX64 = re.compile(r"[a-f0-9]{64}\Z")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def inspect_archive(blob: bytes) -> tuple[dict[str, bytes], str]:
    """Reject traversal, links, duplicate entries and non-regular tar members."""
    if len(blob) > MAX_BYTES:
        raise ValueError("ZIP excede 32 MiB")
    with zipfile.ZipFile(io.BytesIO(blob)) as zip_file:
        if zip_file.namelist() != ["artifact.tar"]:
            raise ValueError("ZIP deve conter somente artifact.tar")
        info = zip_file.getinfo("artifact.tar")
        if info.file_size > MAX_BYTES:
            raise ValueError("TAR excede 32 MiB")
        tar_bytes = zip_file.read(info)
    files: dict[str, bytes] = {}
    entries_seen: set[str] = set()
    total_bytes = 0
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as archive:
        for member in archive:
            name = member.name.removeprefix("./")
            if name in ("", ".") and member.isdir():
                continue
            path = PurePosixPath(name)
            if (not name or name.startswith("/") or "\\" in name
                or any(part in ("", ".", "..") for part in name.split("/"))):
                raise ValueError(f"caminho TAR inseguro: {name!r}")
            if name in entries_seen:
                raise ValueError(f"entrada TAR duplicada: {name}")
            entries_seen.add(name)
            if member.isdir():
                continue
            if not member.isfile():
                raise ValueError(f"TAR contém link ou tipo proibido: {name}")
            total_bytes += member.size
            if total_bytes > MAX_BYTES:
                raise ValueError("arquivos extraídos excedem 32 MiB")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError(f"arquivo TAR ilegível: {name}")
            files[str(path)] = stream.read()
    if "index.html" not in files:
        raise ValueError("index.html ausente")
    if "manifest.json" not in files:
        raise ValueError("manifesto RFB ausente")
    try:
        manifest = json.loads(files["manifest.json"])
    except (ValueError, UnicodeError) as exc:
        raise ValueError("manifesto RFB inválido") from exc
    if not (isinstance(manifest, dict) and isinstance(manifest.get("current"), str)
            and manifest["current"] and isinstance(manifest.get("snapshots"), list)):
        raise ValueError("manifesto sem contrato RFB current/snapshots")
    # Match Node's inventory() in rollback-drill.mjs: JSON.stringify(sorted [path, SHA256]).
    inventory = [[name, digest(data)] for name, data in sorted(files.items())]
    tree_digest = digest(json.dumps(inventory, separators=(",", ":")).encode())
    return files, tree_digest


def extract(blob: bytes, dest: Path, expected_zip: str, expected_tree: str) -> dict:
    if not HEX64.fullmatch(expected_zip) or not HEX64.fullmatch(expected_tree):
        raise ValueError("dois hashes SHA-256 hexadecimais independentes são obrigatórios")
    if digest(blob) != expected_zip:
        raise ValueError("SHA-256 do ZIP divergente")
    files, tree_digest = inspect_archive(blob)
    if tree_digest != expected_tree:
        raise ValueError("SHA-256 da árvore Astro divergente")
    dest = dest.absolute()
    if dest.exists() or dest.is_symlink():
        raise ValueError("destino precisa ser novo e inexistente")
    if not dest.parent.is_dir():
        raise ValueError("diretório pai do destino deve existir")
    tmp = Path(tempfile.mkdtemp(prefix=".ficha-p3a-pages-", dir=dest.parent))
    try:
        for name, data in files.items():
            target = tmp.joinpath(*PurePosixPath(name).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        # No replacement of an existing tree and no deployment; local atomic move.
        os.rename(tmp, dest)
    finally:
        if tmp.exists():
            shutil.rmtree(tmp)
    return {"tree_sha256": tree_digest, "files": len(files), "bytes": sum(map(len, files.values())),
            "manifest_sha256": digest(files["manifest.json"]), "deployed_verified": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_zip", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--expected-zip-sha256", required=True)
    parser.add_argument("--expected-tree-sha256", required=True)
    args = parser.parse_args()
    result = extract(args.artifact_zip.read_bytes(), args.destination,
                     args.expected_zip_sha256, args.expected_tree_sha256)
    print("FICHA_ASTRO_OFFLINE_IMPORT_PASS " + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
