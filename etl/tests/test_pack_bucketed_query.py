from __future__ import annotations

from ficha_etl import pack


class _Cursor:
    description = [("cnpj_base",)]

    def __init__(self, prefix: str) -> None:
        self._batches = [[(f"{prefix}0000001",), (f"{prefix}0000002",)], []]

    def fetchmany(self, _size: int):
        return self._batches.pop(0)


class _Connection:
    def __init__(self) -> None:
        self.prefixes: list[tuple[str, str, str]] = []

    def execute(self, _sql: str, params: list[str]):
        # Company bucket query params are [raizes, p, cnpjs, p, socios, p].
        assert len(params) == 6
        self.prefixes.append((params[1], params[3], params[5]))
        return _Cursor(params[1])


def test_company_row_iterator_processes_disjoint_prefixes_in_order():
    con = _Connection()

    rows = list(
        pack._iter_company_rows(
            con,
            raizes_url="raizes.parquet",
            cnpjs_url="cnpjs.parquet",
            socios_url="socios.parquet",
            batch_size=1,
        )
    )

    assert con.prefixes == [(str(i), str(i), str(i)) for i in range(10)]
    assert [row["cnpj_base"] for row in rows] == [
        value for i in range(10) for value in (f"{i}0000001", f"{i}0000002")
    ]
    # Each of the three large inputs is filtered before its GROUP BY/join.
    assert pack._COMPANIES_SQL.count("WHERE LEFT(cnpj_base, 1) = ?") == 3
