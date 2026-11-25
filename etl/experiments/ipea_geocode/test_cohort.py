"""Synthetic cohort controls; no geocoder or network."""

import csv
import hashlib
import tempfile
import unittest
from pathlib import Path

from cohort import compose, read_cohort, reconcile_candidates

HEADER = ["tipo_logradouro", "logradouro", "numero", "cep", "bairro", "municipio", "uf", "id"]
ROW = ["R", "Av. São João", "10", "00123456", "Centro", "Porto Velho", "RO", "same"]


class TestCohort(unittest.TestCase):
    def report(self, header, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                csv.writer(handle).writerows([header, *rows])
            report = read_cohort(path)
            self.assertEqual(report["input_sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            return report

    def test_duplicate_header(self):
        with self.assertRaisesRegex(ValueError, "Duplicate CSV"):
            self.report(HEADER + ["uf"], [ROW + ["SP"]])

    def test_wrong_width(self):
        for row in (ROW[:-1], ROW + ["extra"]):
            with self.subTest(row=row), self.assertRaisesRegex(ValueError, "Invalid CSV width"):
                self.report(HEADER, [row])

    def test_cohort(self):
        rows = [ROW, ROW.copy(), ROW.copy(), ROW.copy(), ROW.copy()]
        rows[1][1] = ""
        rows[2][6] = ""
        rows[3][5] = ""
        rows[4][1] = " "
        report = self.report(HEADER, rows)
        self.assertEqual([report[k] for k in ("input_rows", "valid_rows", "writer_eligible_rows",
                         "query_eligible_rows", "queried_rows")], [5, 5, 3, 1, 0])
        self.assertEqual(len({r["row_id"] for r in report["records"]}), 5)
        self.assertEqual({r["id"] for r in report["records"]}, {"same"})
        self.assertEqual(report["records"][0]["original_fields"]["cep"], "00123456")
        self.assertEqual(report["records"][0]["query_street"], "Av. São João")
        self.assertEqual(report["prefix_conflict_rows"], 3)

    def test_composition(self):
        for kind, street, expected in [("AV", "Av. São João", ("Av. São João", False)),
                                       ("R", "Av. São João", ("Av. São João", True)),
                                       ("AV", "São João", ("AV São João", False))]:
            self.assertEqual(compose({"tipo_logradouro": kind, "logradouro": street}), expected)

    def test_candidates(self):
        result = reconcile_candidates(["a", "b", "c", "d"],
                                      [("b", "x"), ("a", "z"), ("b", "y"), ("a", "z"), ("d", None)])
        self.assertEqual(result["status"], {"a": "unique", "b": "ambiguous", "c": "unmatched", "d": "unmatched"})
        self.assertEqual(result["counts"], {"unique": 1, "ambiguous": 1, "unmatched": 2})

    def test_invalid_candidate_identity(self):
        for ids, pairs in [(["a", "a"], []), (["a"], [("b", "x")]), (["a"], [("a", "")])]:
            with self.subTest(ids=ids, pairs=pairs), self.assertRaises(ValueError):
                reconcile_candidates(ids, pairs)
