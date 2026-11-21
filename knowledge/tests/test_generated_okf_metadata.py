"""Guard generated OKF Zod metadata while excluding it from SQL row parity.

The original row-shape reader silently dropped type/title/description. Those
members are not Parquet columns, but a changed type literal would still alter
the generated OKF discriminator without failing the v1 compatibility tests.
"""

from __future__ import annotations

import unittest

from test_contract_compatibility import (
    ROOT,
    SOURCES,
    _parse_generated_fields,
    _parse_published_v1_object,
)


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


    def test_negative_commented_shadow_cannot_hide_real_generated_drift(self) -> None:
        # Old re.search selected this stale valid declaration in a block
        # comment, silently overlooking the live wrong discriminator below.
        first = SOURCE.index("export const CnpjSchema = z.object({")
        end = SOURCE.index("\n});", first) + len("\n});")
        stale = SOURCE[first:end]
        changed = SOURCE.replace(
            '"type": z.literal("Cnpj"),',
            '"type": z.literal("Raiz"),',
            1,
        )
        self.assertNotEqual(changed, SOURCE)
        shadowed = "/*\n" + stale + "\n*/\n" + changed
        with self.assertRaisesRegex(AssertionError, "generated OKF metadata changed"):
            _parse_generated_fields(shadowed, "Cnpj")

    def test_negative_commented_shadow_cannot_hide_real_public_drift(self) -> None:
        path = ROOT / "web/src/schemas/v1/estabelecimento.ts"
        source = path.read_text(encoding="utf-8")
        start = source.index("export const EstabelecimentoSchema = z.object({")
        end = source.index("\n});", start) + len("\n});")
        stale = source[start:end]
        changed = source.replace("  cnpj: z.string(),", "  cnpj: z.number(),", 1)
        self.assertNotEqual(changed, source)
        shadowed = "/*\n" + stale + "\n*/\n" + changed
        fields = _parse_published_v1_object(shadowed, "estabelecimento")
        self.assertEqual(fields["cnpj"], "z.number()")

    def test_negative_duplicate_live_exports_fail_closed(self) -> None:
        start = SOURCE.index("export const CnpjSchema = z.object({")
        end = SOURCE.index("\n});", start) + len("\n});")
        duplicate = SOURCE + "\n" + SOURCE[start:end]
        with self.assertRaisesRegex(AssertionError, "exactly one live"):
            _parse_generated_fields(duplicate, "Cnpj")

    def test_negative_unbalanced_block_comment_fails_closed(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unbalanced TypeScript"):
            _parse_generated_fields("/*\n" + SOURCE, "Cnpj")


if __name__ == "__main__":
    unittest.main()
