"""Freeze public v1 column names/kinds independently of SQL and generated Zod.

A synchronized edit to all three artifacts can preserve parity while breaking
the consumer-visible v1 contract. The JSON baseline is intentionally literal;
it must never be regenerated as part of this test.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from test_contract_compatibility import (
    SOURCES,
    _generated_fields,
    _kind,
    _published_v1_fields,
    _sql_fields,
)

BASELINE = json.loads(
    Path(__file__).with_name("public-v1-column-kinds.json").read_text(
        encoding="utf-8"
    )
)


def _shape(fields: dict[str, str]) -> dict[str, str]:
    return {name: _kind(expr) for name, expr in fields.items()}


class PublicV1ColumnBaselineTest(unittest.TestCase):
    def test_names_and_scalar_kinds_are_pinned_independently(self) -> None:
        self.assertEqual(set(BASELINE), set(SOURCES))
        for table, (_, public_name) in SOURCES.items():
            with self.subTest(table=table):
                self.assertEqual(
                    _shape(_published_v1_fields(public_name)),
                    BASELINE[table],
                    f"{table}: public v1 column names/types changed; review versioning",
                )

    def test_negative_joint_rename_can_pass_parity_but_not_baseline(self) -> None:
        sql = _sql_fields("Cnpj", "cnpj")
        generated = _generated_fields("Cnpj")
        published = _published_v1_fields("estabelecimento")
        for fields in (sql, generated, published):
            fields["cnpj_novo"] = fields.pop("cnpj")
        self.assertEqual(sql, _shape(generated))
        self.assertEqual(sql, _shape(published))
        self.assertNotEqual(_shape(published), BASELINE["Cnpj"])

    def test_negative_joint_type_change_can_pass_parity_but_not_baseline(self) -> None:
        sql = _sql_fields("Cnpj", "cnpj")
        generated = _generated_fields("Cnpj")
        published = _published_v1_fields("estabelecimento")
        sql["capital_social"] = "string"
        generated["capital_social"] = "z.string()"
        published["capital_social"] = "z.string()"
        self.assertEqual(sql, _shape(generated))
        self.assertEqual(sql, _shape(published))
        self.assertNotEqual(_shape(published), BASELINE["Cnpj"])


if __name__ == "__main__":
    unittest.main()
