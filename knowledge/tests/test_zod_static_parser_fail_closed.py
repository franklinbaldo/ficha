"""Adversarial guards against silent omissions in the static Zod reader.

This does not execute TypeScript or prove runtime safeParse behavior. Instead,
the offline contract check must reject unsupported object-body syntax rather
than treating comments/spreads as invisible while reporting parity.
"""

from __future__ import annotations

import unittest

from test_contract_compatibility import (
    ROOT,
    _parse_published_v1_object,
    _zod_fields,
)


class ZodStaticParserFailClosedTest(unittest.TestCase):
    def test_block_comment_cannot_resurrect_removed_public_column(self) -> None:
        source = (
            ROOT / "web/src/schemas/v1/estabelecimento.ts"
        ).read_text(encoding="utf-8")
        field = "  cnpj: z.string(),"
        self.assertIn(field, source)
        self.assertIn("cnpj", _parse_published_v1_object(source, "estabelecimento"))
        # In TypeScript this field no longer exists. The old line-based
        # reader still counted it, yielding a false green.
        mutated = source.replace(field, "  /*\n" + field + "\n  */", 1)
        with self.assertRaisesRegex(AssertionError, "Unrecognized Zod object member"):
            _parse_published_v1_object(mutated, "estabelecimento")

    def test_spread_cannot_add_undeclared_public_columns_unnoticed(self) -> None:
        source = (
            ROOT / "web/src/schemas/v1/estabelecimento.ts"
        ).read_text(encoding="utf-8")
        field = "  cnpj: z.string(),"
        self.assertIn(field, source)
        mutated = source.replace(field, "  ...otherColumns,\n" + field, 1)
        with self.assertRaisesRegex(AssertionError, "Unrecognized Zod object member"):
            _parse_published_v1_object(mutated, "estabelecimento")

    def test_generated_object_cannot_count_comment_hidden_member(self) -> None:
        field = '  "cnpj": z.string(),'
        self.assertEqual(_zod_fields(field, quoted=True), {"cnpj": "z.string()"})
        mutated = "  /*\n" + field + "\n  */"
        with self.assertRaisesRegex(AssertionError, "Unrecognized Zod object member"):
            _zod_fields(mutated, quoted=True)


if __name__ == "__main__":
    unittest.main()
