"""Regressoes sintéticas. Sem rede, downloads ou ETL."""
import csv
import tempfile
import unittest
from pathlib import Path
from experiment import ficha_key, run


class TestIpeaExperiment(unittest.TestCase):
    def test_abbreviation(self):
        self.assertEqual(
            ficha_key({"tipo_logradouro": "AV", "logradouro": "São João"}),
            "AVENIDA SAO JOAO",
        )

    def test_explicit_prefix_wins(self):
        self.assertEqual(
            ficha_key({"tipo_logradouro": "R", "logradouro": "Av. São João"}),
            "AVENIDA SAO JOAO",
        )

    def test_street_type_preserved(self):
        self.assertNotEqual(
            ficha_key({"tipo_logradouro": "R", "logradouro": "São João"}),
            ficha_key({"tipo_logradouro": "AV", "logradouro": "São João"}),
        )

    def test_offline_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            columns = ["tipo_logradouro", "logradouro", "numero", "cep",
                       "bairro", "municipio", "uf"]
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerow(dict(zip(columns, [
                    "AV", "São João", "10", "00000000", "Centro",
                    "Porto Velho", "RO",
                ])))
            report = run(path)
            self.assertEqual(report["input_rows"], 1)
            self.assertEqual(report["geocode_status"], "not_run")

    def test_missing_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.csv"
            path.write_text("logradouro\\nRua Teste\\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                run(path)


if __name__ == "__main__":
    unittest.main()
