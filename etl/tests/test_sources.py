import pytest

from ficha_etl import sources


def test_files_for_month_count():
    files = sources.files_for_month("2026-01")
    # 3 big tables × 10 + 7 single tables = 37
    assert len(files) == 37


def test_files_for_month_kinds():
    files = sources.files_for_month("2026-01")
    by_kind: dict[str, int] = {}
    for f in files:
        by_kind[f.kind] = by_kind.get(f.kind, 0) + 1
    assert by_kind["empresas"] == 10
    assert by_kind["estabelecimentos"] == 10
    assert by_kind["socios"] == 10
    for k in ("simples", "cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"):
        assert by_kind[k] == 1


def test_files_for_month_url_shape():
    files = sources.files_for_month("2026-01", base="https://example.com/cnpj")
    sample = next(f for f in files if f.name == "Empresas3.zip")
    assert sample.url == "https://example.com/cnpj/2026-01/Empresas3.zip"


def test_files_for_month_uses_default_base(monkeypatch):
    monkeypatch.delenv("FICHA_RFB_BASE_URL", raising=False)
    files = sources.files_for_month("2026-01")
    assert all(f.url.startswith(sources.DEFAULT_RFB_BASE_URL) for f in files)


def test_files_for_month_respects_env_override(monkeypatch):
    monkeypatch.setenv("FICHA_RFB_BASE_URL", "https://mirror.example.org/rfb")
    files = sources.files_for_month("2026-01")
    assert all(f.url.startswith("https://mirror.example.org/rfb/2026-01/") for f in files)


@pytest.mark.parametrize(
    "bad",
    ["", "2026", "2026-1", "2026-13", "2026-00", "26-01", "abcd-ef", "2026/01"],
)
def test_files_for_month_rejects_bad_format(bad: str):
    with pytest.raises(ValueError):
        sources.files_for_month(bad)


@pytest.mark.parametrize("good", ["2024-01", "2024-12", "2026-04"])
def test_files_for_month_accepts_valid_format(good: str):
    files = sources.files_for_month(good)
    assert len(files) == 37
