"""Execute current generated OKF view SQL on small, independent SQLite fixtures.

SQLite tests only the portable SELECT/JOIN subset used by these two views;
this is not a DuckDB-WASM, Parquet, provenance, or published-site oracle.
"""

import hashlib
import json
from pathlib import Path
import sqlite3
import unittest


GENERATED_VIEWS = (
    Path(__file__).resolve().parents[2] / "web/src/generated/ficha-okf.views.ts"
)


def view_sql_by_name():
    content = GENERATED_VIEWS.read_text(encoding="utf-8")
    marker = "export const okfConvenienceViews = "
    if content.count(marker) != 1:
        raise ValueError("Expected exactly one generated view declaration")
    payload = content.split(marker, 1)[1].split(" as const satisfies", 1)[0]
    views = json.loads(payload)
    if not isinstance(views, list) or len(views) != 2:
        raise ValueError("Unexpected generated view inventory")
    sql = {view["name"]: view["sql"] for view in views}
    if set(sql) != {"empresas_ativas", "socios_de_raizes_ativas"}:
        raise ValueError("Missing or duplicate OKF view names")
    return sql


class GeneratedViewSqlSemanticsTest(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.executescript("""
            CREATE TABLE cnpjs (cnpj TEXT, situacao_cadastral TEXT);
            INSERT INTO cnpjs VALUES ('A', '02'), ('B', '08'), ('C', NULL);
            CREATE TABLE socios (cnpj_base TEXT, nome_socio_razao_social TEXT);
            INSERT INTO socios VALUES
                ('11111111', 'one-active-parent'),
                ('22222222', 'inactive-parent'),
                ('33333333', 'missing-parent'),
                ('44444444', 'duplicate-active-parent'),
                ('55555555', 'null-active-count');
            CREATE TABLE raizes (cnpj_base TEXT, qtd_estabelecimentos_ativos INTEGER);
            INSERT INTO raizes VALUES
                ('11111111', 1), ('22222222', 0),
                ('44444444', 1), ('44444444', 2), ('55555555', NULL);
        """)
        self.sql = view_sql_by_name()

    def test_active_establishments_filters_inactive_and_unknown_status(self):
        rows = self.connection.execute(self.sql["empresas_ativas"]).fetchall()
        self.assertEqual(rows, [("A", "02")])

    def test_join_excludes_missing_zero_and_null_parent(self):
        rows = self.connection.execute(self.sql["socios_de_raizes_ativas"]).fetchall()
        self.assertCountEqual(
            rows,
            [
                ("11111111", "one-active-parent"),
                ("44444444", "duplicate-active-parent"),
                ("44444444", "duplicate-active-parent"),
            ],
        )

    def test_duplicate_active_parent_multiplies_output_not_deduplicated(self):
        rows = self.connection.execute(self.sql["socios_de_raizes_ativas"]).fetchall()
        duplicated = [r for r in rows if r[0] == "44444444"]
        self.assertEqual(len(duplicated), 2)
        self.assertEqual(duplicated[0], duplicated[1])


    def test_competencia_pairing_requires_matching_raiz_snapshot(self):
        # Literal independent inputs: labels do not claim published RFB vintages.
        snapshots = {
            "2026-07": {
                "cnpjs": [("A", "02"), ("B", "08")],
                "socios": [("11111111", "partner-one"), ("22222222", "partner-two")],
                "raizes": [("11111111", 1), ("22222222", 0)],
            },
            "2026-08": {
                "cnpjs": [("A", "08"), ("B", "02")],
                "socios": [("11111111", "partner-one"), ("22222222", "partner-two")],
                "raizes": [("11111111", 0), ("22222222", 1)],
            },
        }
        for competencia, inputs in snapshots.items():
            with self.subTest(competencia=competencia):
                other = next(key for key in snapshots if key != competencia)
                connection = sqlite3.connect(":memory:")
                try:
                    connection.executescript("""
                        CREATE TABLE cnpjs (cnpj TEXT, situacao_cadastral TEXT);
                        CREATE TABLE socios (
                            cnpj_base TEXT, nome_socio_razao_social TEXT
                        );
                        CREATE TABLE raizes (
                            cnpj_base TEXT, qtd_estabelecimentos_ativos INTEGER
                        );
                    """)
                    for relation, rows in inputs.items():
                        connection.executemany(
                            f"INSERT INTO {relation} VALUES (?, ?)", rows
                        )

                    # Per-competence key and parent audits hold for both sets.
                    duplicates = connection.execute("""
                        SELECT cnpj_base FROM raizes GROUP BY cnpj_base
                        HAVING COUNT(*) > 1
                    """).fetchall()
                    orphans = connection.execute("""
                        SELECT s.cnpj_base FROM socios AS s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM raizes AS r
                            WHERE r.cnpj_base = s.cnpj_base
                        )
                    """).fetchall()
                    self.assertEqual(duplicates, [])
                    self.assertEqual(orphans, [])

                    # The oracle reads literal raw tuples, not SQL or Zod output.
                    expected_cnpjs = [
                        row for row in inputs["cnpjs"] if row[1] == "02"
                    ]
                    active = {
                        base
                        for base, count in inputs["raizes"]
                        if count is not None and count > 0
                    }
                    expected_socios = [
                        row for row in inputs["socios"] if row[0] in active
                    ]
                    actual_cnpjs = connection.execute(
                        self.sql["empresas_ativas"]
                    ).fetchall()
                    actual_socios = connection.execute(
                        self.sql["socios_de_raizes_ativas"]
                    ).fetchall()
                    self.assertCountEqual(actual_cnpjs, expected_cnpjs)
                    self.assertCountEqual(actual_socios, expected_socios)

                    # Swapping only Raiz with a different, individually valid
                    # snapshot changes the join even though key audits pass.
                    connection.execute("DELETE FROM raizes")
                    connection.executemany(
                        "INSERT INTO raizes VALUES (?, ?)",
                        snapshots[other]["raizes"],
                    )
                    swapped = connection.execute(
                        self.sql["socios_de_raizes_ativas"]
                    ).fetchall()
                    self.assertNotEqual(sorted(swapped), sorted(expected_socios))
                finally:
                    connection.close()

    def test_equal_outputs_do_not_establish_raiz_competencia(self):
        # Both physical snapshots have valid unique keys and no orphans.
        # Their different positive counts are invisible to SELECT s.*.
        socios = [
            ("11111111", "partner-one"),
            ("22222222", "partner-two"),
        ]
        snapshots = {
            "2026-07": [("11111111", 1), ("22222222", 0)],
            "2026-08": [("11111111", 2), ("22222222", 0)],
        }
        self.connection.execute("DELETE FROM socios")
        self.connection.execute("DELETE FROM raizes")
        self.connection.executemany("INSERT INTO socios VALUES (?, ?)", socios)

        outputs = {}
        digests = {}
        for competencia, raizes in snapshots.items():
            with self.subTest(competencia=competencia):
                self.connection.execute("DELETE FROM raizes")
                self.connection.executemany(
                    "INSERT INTO raizes VALUES (?, ?)", raizes
                )
                duplicates = self.connection.execute("""
                    SELECT cnpj_base FROM raizes GROUP BY cnpj_base
                    HAVING COUNT(*) > 1
                """).fetchall()
                orphans = self.connection.execute("""
                    SELECT s.cnpj_base FROM socios AS s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM raizes AS r
                        WHERE r.cnpj_base = s.cnpj_base
                    )
                """).fetchall()
                self.assertEqual(duplicates, [])
                self.assertEqual(orphans, [])

                # Independent raw-tuple oracle for the current SQL semantics.
                active = {base for base, count in raizes if count > 0}
                expected = [row for row in socios if row[0] in active]
                actual = self.connection.execute(
                    self.sql["socios_de_raizes_ativas"]
                ).fetchall()
                self.assertCountEqual(actual, expected)
                outputs[competencia] = actual
                digests[competencia] = hashlib.sha256(
                    json.dumps(raizes).encode("utf-8")
                ).hexdigest()

        # A real byte-level difference exists, but output parity masks it:
        # consuming August Raiz for July would still pass an output oracle.
        self.assertNotEqual(digests["2026-07"], digests["2026-08"])
        self.assertCountEqual(outputs["2026-07"], outputs["2026-08"])


if __name__ == "__main__":
    unittest.main()
