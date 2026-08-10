from pathlib import Path


transform_path = Path("etl/src/ficha_etl/transform.py")
text = transform_path.read_text()
if "import codecs\n" not in text:
    text = text.replace("import collections\n", "import codecs\nimport collections\n", 1)
if "import tempfile\n" not in text:
    text = text.replace("import shutil\n", "import shutil\nimport tempfile\n", 1)

start = text.index("def _create_table_from_csvs(")
end = text.index("\n\n_OS_HEADROOM_GB", start)
replacement = '''_ENCODING_SCAN_CHUNK_BYTES = 8 * 1024 * 1024


def _is_strict_utf8_file(csv_path: Path) -> bool:
    """Valida UTF-8 sobre o arquivo INTEIRO, em streaming.

    O sniff histórico olhava só o primeiro MiB. Em `Empresas0` de 2026-05
    essa amostra era ASCII puro, embora bytes latin-1 aparecessem depois; a
    decisão prematura mandava o arquivo inteiro para utf-8+ignore_errors e
    descartava linhas válidas. A decisão agora cobre todos os bytes.
    """
    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    try:
        with csv_path.open("rb") as src:
            while chunk := src.read(_ENCODING_SCAN_CHUNK_BYTES):
                decoder.decode(chunk, final=False)
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        return False
    return True


def _transcode_latin1_to_utf8(csv_path: Path) -> Path:
    """Transcodifica ISO-8859-1 → UTF-8 sem descarte nem substituição.

    ISO-8859-1 define um code point para cada byte 0x00..0xFF, inclusive a
    faixa C1 0x80..0x9F que o scanner `latin-1` do DuckDB rejeita. Portanto
    `decode('latin-1').encode('utf-8')` é um mapeamento total e reversível dos
    bytes de origem para texto UTF-8 válido: nenhum registro precisa de
    `ignore_errors=True`.

    O temporário fica no mesmo filesystem do CSV e existe só durante a leitura
    daquele arquivo, limitando o overhead de disco ao maior CSV individual.
    """
    with tempfile.NamedTemporaryFile(
        mode="wb",
        delete=False,
        dir=csv_path.parent,
        prefix=f".{csv_path.name}.ficha-utf8-",
        suffix=".csv",
    ) as dst:
        temp_path = Path(dst.name)
        try:
            with csv_path.open("rb") as src:
                while chunk := src.read(_ENCODING_SCAN_CHUNK_BYTES):
                    dst.write(chunk.decode("latin-1").encode("utf-8"))
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
    return temp_path


def _create_table_from_single_csv(
    con: duckdb.DuckDBPyConnection,
    table: str,
    csv_path: Path,
    spec: registry.CsvSpec,
) -> None:
    """Carrega um CSV RFB sem expor bytes latin-1 crus ao DuckDB.

    Arquivos UTF-8 estritos são lidos diretamente. Qualquer arquivo que não
    seja UTF-8 estrito é primeiro transcodificado de ISO-8859-1 para UTF-8 de
    forma lossless. Em ambos os casos o DuckDB recebe exatamente a mesma
    política: `encoding='utf-8', ignore_errors=false`.
    """
    cleanup: Path | None = None
    if _is_strict_utf8_file(csv_path):
        duckdb_path = csv_path
    else:
        cleanup = _transcode_latin1_to_utf8(csv_path)
        duckdb_path = cleanup
        log.info(
            "transcoded %s ISO-8859-1 → UTF-8 before DuckDB (lossless)",
            csv_path.name,
        )

    try:
        select_sql = registry.read_csv_select_sql(
            spec,
            [duckdb_path],
            encoding="utf-8",
            ignore_errors=False,
        )
        con.execute(f"CREATE OR REPLACE TABLE {table} AS\\n{select_sql}")
    finally:
        if cleanup is not None:
            cleanup.unlink(missing_ok=True)


def _create_table_from_csvs(
    con: duckdb.DuckDBPyConnection,
    table: str,
    csv_paths: Iterable[Path],
    spec: registry.CsvSpec,
) -> None:
    """Cria/recria ``table`` como união lossless de leituras por arquivo.

    O contrato é ``bulk(paths) == UNION(read(path)...)``. Antes de #116, uma
    decisão de encoding tomada pelo primeiro MiB do primeiro arquivo e um
    fallback aplicado ao lote faziam bulk e chunked observarem cardinalidades
    diferentes. Pior: `utf-8 + ignore_errors=True` descartava silenciosamente
    linhas latin-1 válidas.

    Agora cada arquivo passa pela mesma normalização inteira e o DuckDB só lê
    UTF-8 estrito sem `ignore_errors`. Isso preserva bytes C1 (ex. `0x8F` no
    campo `correio_eletronico`) como U+008F em vez de rejeitar ou apagar a
    linha, e mantém arquivos UTF-8 verdadeiros sem mojibake.
    """
    paths = [p for p in csv_paths if p.exists() and p.stat().st_size > 0]
    if not paths:
        col_defs = ", ".join(f"{c} VARCHAR" for c in spec.columns)
        con.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")
        return

    _create_table_from_single_csv(con, table, paths[0], spec)
    staging = f"__ficha_{table}_csv_part"
    try:
        for csv_path in paths[1:]:
            _create_table_from_single_csv(con, staging, csv_path, spec)
            con.execute(f"INSERT INTO {table} SELECT * FROM {staging}")
    finally:
        con.execute(f"DROP TABLE IF EXISTS {staging}")
'''
text = text[:start] + replacement + text[end:]

lookup_start = text.index("def load_lookup_into_duckdb(")
lookup_end = text.index("\n\ndef lookups_dict", lookup_start)
lookup_replacement = '''def load_lookup_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    kind: FileKind,
    csv_path: Path,
) -> None:
    """Carrega lookup RFB pelo mesmo caminho lossless das tabelas principais."""
    table = f"lookup_{kind}"
    spec = registry.CsvSpec(columns=("codigo", "descricao"))
    _create_table_from_csvs(con, table, [csv_path], spec)
'''
text = text[:lookup_start] + lookup_replacement + text[lookup_end:]
transform_path.write_text(text)


registry_path = Path("etl/src/ficha_etl/registry.py")
registry_text = registry_path.read_text()
registry_text = registry_text.replace(
    "A orquestração (I/O, tentativas de encoding, escrita e métricas) continua fora\n"
    "deste módulo. As funções daqui são puras e determinísticas.\n",
    "A orquestração (I/O, normalização de encoding, escrita e métricas) continua fora\n"
    "deste módulo. As funções daqui são puras e determinísticas.\n",
)
encoding_start = registry_text.find("\n\ndef encoding_attempts(")
if encoding_start != -1:
    registry_text = registry_text[:encoding_start].rstrip() + "\n"
registry_path.write_text(registry_text)


test_registry_path = Path("etl/tests/test_registry.py")
test_registry = test_registry_path.read_text()
test_registry = test_registry.replace(
    "  c) encoding_attempts — mesma semântica do sniff atual em transform.py.\n"
    "  d) Fixtures comportamentais load-bearing via _create_table_from_csvs real.\n"
    "  e) paths_literal — escaping de apóstrofo.\n"
    "  f) _create_table_from_csvs honra o CsvSpec passado (não reconstrói com\n",
    "  c) Fixtures comportamentais load-bearing via _create_table_from_csvs real.\n"
    "  d) paths_literal — escaping de apóstrofo.\n"
    "  e) _create_table_from_csvs honra o CsvSpec passado (não reconstrói com\n",
)
section_start = test_registry.find(
    "# -----------------------------------------------------------------------------\n"
    "# c) encoding_attempts\n"
)
section_end = test_registry.find(
    "# -----------------------------------------------------------------------------\n"
    "# e) paths_literal\n",
    section_start,
)
if section_start != -1 and section_end != -1:
    test_registry = (
        test_registry[:section_start]
        + "# -----------------------------------------------------------------------------\n# d) paths_literal\n"
        + test_registry[section_end + len(
            "# -----------------------------------------------------------------------------\n# e) paths_literal\n"
        ):]
    )
test_registry_path.write_text(test_registry)


test_path = Path("etl/tests/test_encoding_invariance_116.py")
test_path.write_text(r'''from pathlib import Path

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
    paths[0].write_bytes(_csv_line(["01", "ASCII"]));
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
            individual.extend(
                con.execute(f"SELECT codigo, descricao FROM single_{i}").fetchall()
            )
    finally:
        con.close()

    assert bulk == sorted(individual)
    assert bulk == [("01", "ASCII"), ("02", "café \x8f"), ("03", "ação UTF-8")]
    assert calls
    assert all(encoding == "utf-8" and ignore_errors is False for encoding, ignore_errors in calls)
    assert not list(tmp_path.glob(".*.ficha-utf8-*.csv"))
''')
