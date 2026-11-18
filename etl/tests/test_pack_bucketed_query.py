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

    # Coverage includes roots with letters, is ordered and never overlaps.
    assert len(con.ranges) == 36**2
    assert con.ranges[0][0] == "00000000"
    assert con.ranges[-1][1] == "[0000000"
    for previous, current in zip(con.ranges, con.ranges[1:]):
        assert previous[1] == current[0]
    assert [row["cnpj_base"] for row in rows] == sorted(row["cnpj_base"] for row in rows)
    assert any(row["cnpj_base"].startswith("AB") for row in rows)
    # Each of the three large inputs is range-filtered before GROUP BY/join.
    assert pack._COMPANIES_SQL.count("WHERE cnpj_base >= ? AND cnpj_base < ?") == 3
