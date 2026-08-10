from pathlib import Path

import duckdb

from ficha_etl import registry, transform


def _write(path: Path, code: str) -> None:
    path.write_bytes(f'"{code}";"descricao"\n'.encode("latin-1"))


def test_bulk_csv_read_resolves_encoding_per_file(tmp_path, monkeypatch):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    _write(first, "01")
    _write(second, "02")

    spec = registry.CsvSpec(columns=("codigo", "descricao"))
    calls = []
    original = registry.read_csv_select_sql

    def spy(spec_arg, paths, *, encoding, ignore_errors):
        path_list = list(paths)
        calls.append((tuple(path_list), encoding, ignore_errors))
        return original(spec_arg, path_list, encoding=encoding, ignore_errors=ignore_errors)

    monkeypatch.setattr(registry, "read_csv_select_sql", spy)
    con = duckdb.connect()
    transform._create_table_from_csvs(con, "lookup_test", [first, second], spec)

    assert con.execute("SELECT COUNT(*) FROM lookup_test").fetchone()[0] == 2
    assert {row[0] for row in con.execute("SELECT codigo FROM lookup_test").fetchall()} == {
        "01",
        "02",
    }
    # A decisão de encoding deve ser local a cada arquivo. No código pré-#116,
    # read_csv_select_sql recebe os dois paths de uma vez, então um arquivo
    # problemático pode impor o fallback (inclusive ignore_errors=True) ao lote.
    assert calls
    assert all(len(paths) == 1 for paths, _encoding, _ignore in calls)


def test_bulk_count_equals_sum_of_individual_reads(tmp_path):
    paths = [tmp_path / f"part-{i}.csv" for i in range(3)]
    for i, path in enumerate(paths):
        _write(path, f"0{i}")

    spec = registry.CsvSpec(columns=("codigo", "descricao"))
    con = duckdb.connect()
    transform._create_table_from_csvs(con, "bulk", paths, spec)
    bulk = con.execute("SELECT COUNT(*) FROM bulk").fetchone()[0]

    individual = 0
    for i, path in enumerate(paths):
        transform._create_table_from_csvs(con, f"single_{i}", [path], spec)
        individual += con.execute(f"SELECT COUNT(*) FROM single_{i}").fetchone()[0]

    assert bulk == individual == len(paths)
