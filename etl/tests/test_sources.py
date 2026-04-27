import pytest

from ficha_etl import sources


def test_canonical_inventory_count():
    inv = sources.canonical_inventory()
    # 3 big tables × 10 + 7 single tables = 37
    assert len(inv) == 37


def test_canonical_inventory_kinds():
    inv = sources.canonical_inventory()
    by_kind: dict[str, int] = {}
    for spec in inv:
        by_kind[spec.kind] = by_kind.get(spec.kind, 0) + 1
    assert by_kind["empresas"] == 10
    assert by_kind["estabelecimentos"] == 10
    assert by_kind["socios"] == 10
    for k in ("simples", "cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"):
        assert by_kind[k] == 1


def test_canonical_inventory_filenames_sample():
    inv = sources.canonical_inventory()
    names = {spec.name for spec in inv}
    assert "Empresas3.zip" in names
    assert "Estabelecimentos9.zip" in names
    assert "Cnaes.zip" in names
    assert "Qualificacoes.zip" in names


@pytest.mark.parametrize(
    "bad",
    ["", "2026", "2026-1", "2026-13", "2026-00", "26-01", "abcd-ef", "2026/01"],
)
def test_is_valid_month_rejects_bad(bad: str):
    assert sources.is_valid_month(bad) is False


@pytest.mark.parametrize("good", ["2024-01", "2024-12", "2026-04"])
def test_is_valid_month_accepts_good(good: str):
    assert sources.is_valid_month(good) is True
