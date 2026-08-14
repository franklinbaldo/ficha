from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


def main() -> None:
    database = Path(sys.argv[1])
    con = duckdb.connect(str(database), read_only=True)
    rows = con.execute(
        """
        SELECT path, frontmatter_json
        FROM okf.concepts
        WHERE concept_type = 'View'
        ORDER BY path
        """
    ).fetchall()

    views: list[dict[str, object]] = []
    names: set[str] = set()
    for path, raw in rows:
        fm = json.loads(raw)
        name = fm.get("name")
        sql = fm.get("sql")
        inputs = fm.get("inputs")
        output = fm.get("output")
        purpose = fm.get("purpose")
        if not isinstance(name, str) or not name:
            raise ValueError(f"View sem name: {path}")
        if name in names:
            raise ValueError(f"View duplicada: {name}")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError(f"View sem sql: {path}")
        if not isinstance(inputs, list) or not all(isinstance(item, str) for item in inputs):
            raise ValueError(f"View com inputs invalidos: {path}")
        if not isinstance(output, str) or not output:
            raise ValueError(f"View sem output: {path}")
        if purpose is not None and not isinstance(purpose, str):
            raise ValueError(f"View com purpose invalido: {path}")
        names.add(name)
        views.append(
            {
                "name": name,
                "inputs": inputs,
                "output": output,
                "purpose": purpose,
                "sql": sql.strip(),
            }
        )

    payload = json.dumps(views, ensure_ascii=False, indent=2)
    print("// Generated from knowledge/ by scripts/generate-okf-contracts.sh. Do not edit.")
    print("export type OkfConvenienceView = {")
    print("  readonly name: string;")
    print("  readonly inputs: readonly string[];")
    print("  readonly output: string;")
    print("  readonly purpose: string | null;")
    print("  readonly sql: string;")
    print("};")
    print()
    print(f"export const okfConvenienceViews = {payload} as const satisfies readonly OkfConvenienceView[];")


if __name__ == "__main__":
    main()
