import unittest
from lexical import compare


def record(i, base, query, city="A", eligible=True):
    return {"row_id": f"row-{i}", "id": "dup", "query_eligible": eligible,
            "ficha_normalizado": base, "query_street": query,
            "original_fields": {"uf": "RO", "municipio": city, "cep": "00123456"}}


class TestAccounting(unittest.TestCase):
    def test_merge_split_and_regions(self):
        rows = [record(0, "A", "X"), record(1, "B", "X"), record(2, "A", "Y"),
                record(3, "C", "X", "B"), record(4, "D", "Z", eligible=False)]
        report = {"records": rows, "input_rows": 5, "input_sha256": "synthetic"}
        result = compare(report, lambda query: query)
        self.assertEqual(result["compared_rows"], 4)
        self.assertEqual(result["baseline_split_groups"], 1)
        self.assertEqual(result["ipea_merged_groups"], 1)
        self.assertEqual(result["different_rows"], 4)
        self.assertEqual([r["row_id"] for r in result["records"]], ["row-0", "row-1", "row-2", "row-3"])
        self.assertEqual(result["records"][0]["original_fields"]["cep"], "00123456")
        self.assertNotIn("ipea_normalizado", rows[0])

    def test_empty_and_equal(self):
        report = {"records": [record(0, "A", "A"), record(1, "B", "")],
                  "input_rows": 2, "input_sha256": "synthetic"}
        result = compare(report, lambda query: query)
        self.assertEqual((result["equal_rows"], result["different_rows"], result["empty_ipea_rows"]), (1, 1, 1))

    def test_invalid_output(self):
        with self.assertRaises(TypeError):
            compare({"records": [record(0, "A", "A")], "input_rows": 1, "input_sha256": "synthetic"}, lambda _: None)
