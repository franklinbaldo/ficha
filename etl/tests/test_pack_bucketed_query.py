from __future__ import annotations

from ficha_etl import pack


class _Cursor:
    description = [("cnpj_base",)]

    def __init__(self, lower: str) -> None:
        prefix = lower[:2]
        self._batches = [[(f"{prefix}000001",), (f"{prefix}000002",)], []]

    def fetchmany(self, _size: int):
        return self._batches.pop(0)


class _Connection:
    def __init__(self) -> None:
        self.ranges: list[tuple[str, str, str, str, str, str]] = []

    def execute(self, _sql: str, params: list[str]):
        # Params are [raizes, lo, hi, cnpjs, lo, hi, socios, lo, hi].
        assert len(params) == 9
        self.ranges.append((params[1], params[2], params[4], params[5], params[7], params[8]))
        return _Cursor(params[1])


def test_company_row_iterator_processes_disjoint_ranges_in_order():
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

    expected_ranges = []
    expected_rows = []
    for i in range(100):
        prefix = f"{i:02d}"
        lower = f"{prefix}000000"
        upper = f"{i + 1:02d}000000" if i < 99 else "A0000000"
        expected_ranges.append((lower, upper, lower, upper, lower, upper))
        expected_rows.extend((f"{prefix}000001", f"{prefix}000002"))

    assert con.ranges == expected_ranges
    assert [row["cnpj_base"] for row in rows] == expected_rows
    # Each of the three large inputs is range-filtered before GROUP BY/join.
    assert pack._COMPANIES_SQL.count("WHERE cnpj_base >= ? AND cnpj_base < ?") == 3
