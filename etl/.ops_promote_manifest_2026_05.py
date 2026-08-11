"""Promove `2026-05` ao manifesto público a partir dos outputs locais — #110.

Retomada, não recomputação: os artefatos derivados de `2026-05` já são duráveis
em `ia:ficha-2026-05` desde o run 31450937194 (só faltava `companies.zip`).
Este script executa exatamente o estágio 5/5 do pipeline sobre o diretório de
output já materializado, sem tocar em nenhum gate:

  build_snapshot_entry()  → exige os 9 artefatos locais (inclusive companies.zip)
  verify_snapshot_files() → HEAD em toda URL declarada, com conferência de tamanho
  update_manifest()       → só depois de as duas etapas acima passarem
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from ficha_etl import manifest as manifest_mod

MONTH = "2026-05"
OUTPUT_DIR = Path(".cache") / MONTH / "output"
MANIFEST_PATH = Path("..") / "web" / "public" / "manifest.json"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    print(f"[manifest 1/3] build_snapshot_entry({MONTH}, {OUTPUT_DIR})", flush=True)
    entry = manifest_mod.build_snapshot_entry(MONTH, OUTPUT_DIR)

    print("[manifest 2/3] verify_snapshot_files — HEAD em todas as URLs", flush=True)
    broken = manifest_mod.verify_snapshot_files(entry)
    if broken:
        print(
            "error: manifest não promovido — declarados mas inacessíveis no IA:\n"
            + "\n".join(f"  {u}" for u in broken),
            file=sys.stderr,
        )
        return 1

    print(f"[manifest 3/3] update_manifest({MANIFEST_PATH})", flush=True)
    manifest_mod.update_manifest(MANIFEST_PATH, entry)

    print(f"\npromovido — {MONTH} row_counts={entry['row_counts']}", flush=True)
    print(f"files.cnpjs.size={entry['files']['cnpjs']['size']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
