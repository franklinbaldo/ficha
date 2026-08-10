from pathlib import Path

import duckdb

from ficha_etl import registry, transform


def _csv_line(values: list[str], *, encoding: str = "latin-1") -> bytes:
    text = ";".join(f'"{value}"' for value in values) + "\n"
    return text.encode(encoding)


def test_real_c1_shape_is_preserved_without_ignore_errors(tmp_path, monkeypatch):
    """Reproduz o padrão real de 2026-05: email = byte C1 0x8F."""
    csv_path = tmp_path / "estabelecimento-c1.csv"
    row = [""] * len(registry.ESTABELECIMENTO_COLUMNS)
    row[0] = "12345678"
    row[1] = "0001"
    row[2] = "90"
    row[5] = "02"
    row[14] = "RUA JOSÉ"
    row[17] = "SÃO JOÃO"
    row[27] = "\x8f"
    csv_path.write_bytes(_csv_line(row))

    calls: list[tuple[str, bool]] = []
    original = registry.read_csv_select_sql

    def spy(spec, paths, *, encoding, ignore_errors):
        calls.append((encoding, ignore_errors))
        return original(spec, paths, encoding=encoding, ignore_errors=ignore_errors)

    monkeypatch.setattr(registry, "read_csv_select_sql", spy)
    con = duckdb.connect()
    try:
        transform._create_table_from_csvs(
            con,
            "estabelecimento",
            [csv_path],
            registry.main_table("estabelecimento").source,
        )
        actual = con.execute(
            "SELECT logradouro, bairro, correio_eletronico FROM estabelecimento"
        ).fetchone()
    finally:
        con.close()

    assert actual == ("RUA JOSÉ", "SÃO JOÃO", "\x8f")
    assert calls == [("utf-8", False)]
    assert not list(tmp_path.glob(".*.ficha-utf8-*.csv"))


def test_latin1_after_first_megabyte_is_not_silently_dropped(tmp_path):
    """Regressão do sniff de 1 MiB: o byte latin-1 aparece só no fim."""
    csv_path = tmp_path / "late-latin1.csv"
    ascii_row = b'"01";"ASCII ONLY"\n'
    repetitions = (1024 * 1024 // len(ascii_row)) + 10
    with csv_path.open("wb") as f:
        f.write(ascii_row * repetitions)
        f.write(b'"99";"caf\xe9"\n')

    con = duckdb.connect()
    try:
        spec = registry.CsvSpec(columns=("codigo", "descricao"))
        transform._create_table_from_csvs(con, "late", [csv_path], spec)
        count = con.execute("SELECT COUNT(*) FROM late").fetchone()[0]
        tail = con.execute("SELECT descricao FROM late WHERE codigo = '99'").fetchone()[0]
    finally:
        con.close()

    assert count == repetitions + 1
    assert tail == "café"
    assert not list(tmp_path.glob(".*.ficha-utf8-*.csv"))


def test_true_utf8_file_is_not_mojibaked(tmp_path, monkeypatch):
    csv_path = tmp_path / "utf8.csv"
    csv_path.write_bytes(_csv_line(["01", "São José – UTF-8"], encoding="utf-8"))

    calls: list[tuple[tuple[Path, ...], str, bool]] = []
    original = registry.read_csv_select_sql

    def spy(spec, paths, *, encoding, ignore_errors):
        calls.append((tuple(paths), encoding, ignore_errors))
        return original(spec, paths, encoding=encoding, ignore_errors=ignore_errors)

    monkeypatch.setattr(registry, "read_csv_select_sql", spy)
    con = duckdb.connect()
    try:
        spec = registry.CsvSpec(columns=("codigo", "descricao"))
        transform._create_table_from_csvs(con, "utf8_data", [csv_path], spec)
        value = con.execute("SELECT descricao FROM utf8_data").fetchone()[0]
    finally:
        con.close()

    assert value == "São José – UTF-8"
    assert calls == [((csv_path,), "utf-8", False)]


def test_bulk_equals_union_for_ascii_latin1_c1_and_utf8(tmp_path, monkeypatch):
    paths = [tmp_path / f"part-{i}.csv" for i in range(3)]
    paths[0].write_bytes(_csv_line(["01", "ASCII"]))
    paths[1].write_bytes(b'"02";"caf\xe9 \x8f"\n')
    paths[2].write_bytes(_csv_line(["03", "ação UTF-8"], encoding="utf-8"))

    calls: list[tuple[str, bool]] = []
    original = registry.read_csv_select_sql

    def spy(spec, read_paths, *, encoding, ignore_errors):
        calls.append((encoding, ignore_errors))
        return original(spec, read_paths, encoding=encoding, ignore_errors=ignore_errors)

    monkeypatch.setattr(registry, "read_csv_select_sql", spy)
    con = duckdb.connect()
    try:
        spec = registry.CsvSpec(columns=("codigo", "descricao"))
        transform._create_table_from_csvs(con, "bulk", paths, spec)
        bulk = con.execute("SELECT codigo, descricao FROM bulk ORDER BY codigo").fetchall()

        individual = []
        for i, path in enumerate(paths):
            transform._create_table_from_csvs(con, f"single_{i}", [path], spec)
            individual.extend(con.execute(f"SELECT codigo, descricao FROM single_{i}").fetchall())
    finally:
        con.close()

    assert bulk == sorted(individual)
    assert bulk == [("01", "ASCII"), ("02", "café \x8f"), ("03", "ação UTF-8")]
    assert calls
    assert all(encoding == "utf-8" and ignore_errors is False for encoding, ignore_errors in calls)
    assert not list(tmp_path.glob(".*.ficha-utf8-*.csv"))
