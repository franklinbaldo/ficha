"""Offline OKF-to-public-v1 contract guard (no Parquet generation or network).

This detects column/type drift and tracks the existing nullable gap explicitly.
It does NOT assert that the generated OKF Zod accepts every published row.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCES = {
    "Cnpj": ("cnpj", "estabelecimento"),
    "Raiz": ("raiz", "raiz"),
    "Socio": ("socio", "socio"),
}
SQL_KINDS = {
    "VARCHAR": "string",
    "VARCHAR[]": "string[]",
    "DOUBLE": "number",
    "INTEGER": "integer",
    "BOOLEAN": "boolean",
}
# Published v1 permits null, but the current generated OKF schema does not.
# This debt inventory is an explicit compatibility baseline, NOT an allowlist
# authorizing validation of these rows using the stricter generated schema.
KNOWN_NULLABILITY_GAPS = {
    "Cnpj": {
        "ente_federativo_responsavel",
        "nome_fantasia",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "pais_codigo",
        "pais_nome",
        "nome_cidade_exterior",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "opcao_simples",
        "data_opcao_simples",
        "opcao_mei",
    },
    "Raiz": {"ente_federativo_responsavel"},
    "Socio": {
        "cnpj_socio",
        "pais_codigo",
        "pais_nome",
        "representante_legal_qualificacao_descricao",
        "faixa_etaria",
    },
}


# Enum domains are stricter than SQL VARCHAR; generated Zod currently widens them.
EXPECTED_PUBLIC_ENUM_DOMAINS = {
    "Cnpj": {
        "identificador_matriz_filial": ("1", "2"),
        "porte_empresa": ("00", "01", "03", "05"),
        "situacao_cadastral": ("01", "02", "03", "04", "08"),
    },
    "Raiz": {"porte_empresa": ("00", "01", "03", "05")},
    "Socio": {
        "tipo": ("1", "2", "3"),
        "tipo_descricao": ("PJ", "PF", "estrangeiro"),
        "faixa_etaria": ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
    },
}
KNOWN_UNRESTRICTED_ENUM_GAPS = {
    table: set(domains) for table, domains in EXPECTED_PUBLIC_ENUM_DOMAINS.items()
}


def _sql_fields(table: str, slug: str) -> dict[str, str]:
    source = (ROOT / f"knowledge/types/{slug}.schema.sql").read_text(encoding="utf-8")
    match = re.fullmatch(
        r'\s*CREATE TABLE "' + re.escape(table) + r'"\s*\((.*?)\);\s*',
        source,
        flags=re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"Unexpected SQL declaration: {table}")
    fields = {}
    for line in match.group(1).splitlines():
        if not line.strip():
            continue
        parsed = re.fullmatch(r"\s*(\w+)\s+(VARCHAR(?:\[\])?|DOUBLE|INTEGER|BOOLEAN)\s*,?\s*", line)
        if parsed is None:
            raise AssertionError(f"Unrecognized SQL column declaration: {line}")
        name, kind = parsed.groups()
        if name in fields:
            raise AssertionError(f"Duplicate SQL column: {table}.{name}")
        fields[name] = SQL_KINDS[kind]
    return fields


def _zod_fields(source: str, *, quoted: bool) -> dict[str, str]:
    fields = {}
    pattern = (
        r'^\s*"(\w+)":\s*(z\..+),?$'
        if quoted
        else r"^\s*(\w+):\s*(z\..+),$"
    )
    for line in source.splitlines():
        parsed = re.match(pattern, line)
        if parsed is not None:
            name, value = parsed.groups()
            if name in fields:
                raise AssertionError(f"Duplicate Zod field: {name}")
            fields[name] = value.removesuffix(",")
    return fields


def _generated_fields(table: str) -> dict[str, str]:
    source = (ROOT / "web/src/generated/ficha-okf.zod.ts").read_text(encoding="utf-8")
    match = re.search(
        rf"export const {table}Schema = z\.object\(\{{(.*?)\}}\);",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"Missing generated Zod schema: {table}")
    fields = _zod_fields(match.group(1), quoted=True)
    for metadata in ("type", "title", "description"):
        fields.pop(metadata, None)
    return fields


def _published_v1_fields(type_name: str) -> dict[str, str]:
    source = (
        ROOT / f"web/src/schemas/v1/{type_name}.ts"
    ).read_text(encoding="utf-8")
    return _zod_fields(source, quoted=False)


def _kind(expr: str) -> str:
    expr = expr.removesuffix(".nullable()").removesuffix(".optional()")
    if expr == "z.string()" or expr.startswith("z.enum("):
        return "string"
    if expr == "z.number().int()":
        return "integer"
    if expr == "z.number()":
        return "number"
    if expr == "z.boolean()":
        return "boolean"
    if expr == "z.array(z.string())":
        return "string[]"
    raise AssertionError(f"Unknown Zod scalar type: {expr}")


def _enum_domain(expr: str) -> tuple[str, ...] | None:
    if not expr.startswith("z.enum("):
        return None
    match = re.fullmatch(r"z\.enum\(\[([^\]]+)\]\)(?:\.nullable\(\))?", expr)
    if match is None:
        raise AssertionError(f"Unexpected Zod enum expression: {expr}")
    members = [item.strip() for item in match.group(1).split(",")]
    if not all(re.fullmatch(r"'[^']+'", item) for item in members):
        raise AssertionError(f"Unexpected Zod enum members: {expr}")
    values = tuple(item[1:-1] for item in members)
    if len(values) != len(set(values)):
        raise AssertionError(f"Duplicate Zod enum member: {expr}")
    return values


def _public_enum_domains(fields: dict[str, str]) -> dict[str, tuple[str, ...]]:
    return {
        name: values
        for name, expr in fields.items()
        if (values := _enum_domain(expr)) is not None
    }


def _nullability_gap(
    published: dict[str, str], generated: dict[str, str]
) -> set[str]:
    return {
        name
        for name, expr in published.items()
        if expr.endswith(".nullable()")
        and name in generated
        and not generated[name].endswith(".nullable()")
    }



def _optional_mismatch(
    published: dict[str, str], generated: dict[str, str]
) -> set[str]:
    """Catch a new acceptance of undefined/missing fields, distinct from null."""
    return {
        name
        for name, expr in published.items()
        if (".optional()" in expr) != (".optional()" in generated[name])
    }


def _optional_fields(fields: dict[str, str]) -> set[str]:
    """All missing/undefined-enabled fields, even if both schemas agree."""
    return {name for name, expr in fields.items() if ".optional()" in expr}


class OkfContractCompatibilityTest(unittest.TestCase):
    def test_sql_generated_and_public_v1_column_types_agree(self) -> None:
        for table, (slug, public_name) in SOURCES.items():
            with self.subTest(table=table):
                sql = _sql_fields(table, slug)
                generated = _generated_fields(table)
                published = _published_v1_fields(public_name)
                self.assertGreater(len(sql), 0)
                self.assertEqual(set(sql), set(generated), f"{table}: SQL vs OKF Zod")
                self.assertEqual(set(sql), set(published), f"{table}: SQL vs public v1")
                self.assertEqual(sql, {name: _kind(expr) for name, expr in generated.items()})
                self.assertEqual(sql, {name: _kind(expr) for name, expr in published.items()})

    def test_nullable_contract_debt_is_explicit_and_does_not_grow(self) -> None:
        for table, (_, public_name) in SOURCES.items():
            with self.subTest(table=table):
                generated = _generated_fields(table)
                published = _published_v1_fields(public_name)
                self.assertEqual(
                    _nullability_gap(published, generated),
                    KNOWN_NULLABILITY_GAPS[table],
                    "Review the mismatch against published v1 before changing this baseline",
                )
                self.assertFalse(
                    {
                        field for field, expr in generated.items()
                        if expr.endswith(".nullable()")
                        and not published[field].endswith(".nullable()")
                    },
                    f"{table}: generated schema unexpectedly widened nullability",
                )


    def test_required_field_presence_matches_public_v1(self) -> None:
        # Generated Zod metadata (title/description) is intentionally excluded.
        for table, (_, public_name) in SOURCES.items():
            with self.subTest(table=table):
                generated = _generated_fields(table)
                published = _published_v1_fields(public_name)
                self.assertEqual(
                    _optional_mismatch(published, generated),
                    set(),
                    f"{table}: unexpected required/optional field drift",
                )
                # The public v1 contract requires all 86 data columns. A
                # simultaneous .optional() change in both schemas must fail,
                # not pass merely because their optionality matches.
                self.assertFalse(
                    _optional_fields(published),
                    f"{table}: public v1 unexpectedly allows missing columns",
                )
                self.assertFalse(
                    _optional_fields(generated),
                    f"{table}: generated OKF unexpectedly allows missing columns",
                )

    def test_negative_optional_field_widening_would_be_detected(self) -> None:
        published = _published_v1_fields("estabelecimento")
        generated = _generated_fields("Cnpj")
        generated["cnpj"] = "z.string().optional()"
        self.assertEqual(_optional_mismatch(published, generated), {"cnpj"})

    def test_negative_both_schemas_widened_to_optional_is_detected(self) -> None:
        published = _published_v1_fields("estabelecimento")
        generated = _generated_fields("Cnpj")
        published["cnpj"] = "z.string().optional()"
        generated["cnpj"] = "z.string().optional()"
        # A parity-only assertion would miss this breaking change.
        self.assertEqual(_optional_mismatch(published, generated), set())
        self.assertEqual(_optional_fields(published), {"cnpj"})
        self.assertEqual(_optional_fields(generated), {"cnpj"})

    def test_public_enum_domains_and_generated_widening_are_explicit(self) -> None:
        for table, (_, public_name) in SOURCES.items():
            with self.subTest(table=table):
                published = _published_v1_fields(public_name)
                generated = _generated_fields(table)
                domains = _public_enum_domains(published)
                self.assertEqual(domains, EXPECTED_PUBLIC_ENUM_DOMAINS[table])
                widened = {
                    name
                    for name in domains
                    if _enum_domain(generated[name]) is None
                }
                self.assertEqual(widened, KNOWN_UNRESTRICTED_ENUM_GAPS[table])
                for name, values in domains.items():
                    if name not in widened:
                        self.assertEqual(
                            _enum_domain(generated[name]),
                            values,
                            f"{table}.{name}: generated enum has different allowed values",
                        )

    def test_negative_public_enum_domain_drift_would_be_detected(self) -> None:
        published = _published_v1_fields("socio")
        published["tipo"] = "z.enum(['1', '2'])"
        self.assertNotEqual(
            _public_enum_domains(published),
            EXPECTED_PUBLIC_ENUM_DOMAINS["Socio"],
        )

    def test_negative_wrong_type_would_be_detected(self) -> None:
        published = _published_v1_fields("estabelecimento")
        generated = _generated_fields("Cnpj")
        generated["cnpj"] = "z.number()"
        self.assertNotEqual(_kind(published["cnpj"]), _kind(generated["cnpj"]))

    def test_negative_new_nullable_gap_would_be_detected(self) -> None:
        published = _published_v1_fields("socio")
        generated = _generated_fields("Socio")
        generated["cpf_mascarado"] = "z.string()"
        self.assertIn("cpf_mascarado", _nullability_gap(published, generated))
        self.assertNotEqual(
            _nullability_gap(published, generated),
            KNOWN_NULLABILITY_GAPS["Socio"],
        )


if __name__ == "__main__":
    unittest.main()
