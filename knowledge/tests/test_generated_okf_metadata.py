"""Guard generated OKF Zod metadata while excluding it from SQL row parity.

The original row-shape reader silently dropped type/title/description. Those
members are not Parquet columns, but a changed type literal would still alter
the generated OKF discriminator without failing the v1 compatibility tests.
"""

from __future__ import annotations

import unittest

from test_contract_compatibility import ROOT, SOURCES, _parse_generated_fields


SOURCE = (ROOT / "web/src/generated/ficha-okf.zod.ts").read_text(encoding="utf-8")


class GeneratedOkfMetadataTest(unittest.TestCase):
    def test_current_generated_discriminators_and_metadata_are_exact(self) -> None:
        for table in SOURCES:
            with self.subTest(table=table):
                self.assertTrue(_parse_generated_fields(SOURCE, table))

    def test_negative_wrong_type_literal_cannot_pass_row_parity(self) -> None:
        original = '"type": z.literal("Cnpj"),'
        self.assertIn(original, SOURCE)
        mutated = SOURCE.replace(original, '"type": z.literal("Raiz"),', 1)
        with self.assertRaisesRegex(AssertionError, "generated OKF metadata changed"):
            _parse_generated_fields(mutated, "Cnpj")

    def test_negative_deleted_type_cannot_pass_row_parity(self) -> None:
        original = '  "type": z.literal("Cnpj"),\n'
        self.assertIn(original, SOURCE)
        mutated = SOURCE.replace(original, "", 1)
        with self.assertRaisesRegex(AssertionError, "generated OKF metadata changed"):
            _parse_generated_fields(mutated, "Cnpj")

    def test_negative_required_title_cannot_pass_row_parity(self) -> None:
        original = '"title": z.string().optional(),'
        self.assertIn(original, SOURCE)
        mutated = SOURCE.replace(original, '"title": z.string(),', 1)
        with self.assertRaisesRegex(AssertionError, "generated OKF metadata changed"):
            _parse_generated_fields(mutated, "Cnpj")

    def test_negative_description_type_change_cannot_pass_row_parity(self) -> None:
        original = '"description": z.string().optional(),'
        self.assertIn(original, SOURCE)
        mutated = SOURCE.replace(original, '"description": z.number(),', 1)
        with self.assertRaisesRegex(AssertionError, "generated OKF metadata changed"):
            _parse_generated_fields(mutated, "Cnpj")


if __name__ == "__main__":
    unittest.main()
