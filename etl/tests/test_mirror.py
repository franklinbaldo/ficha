import pytest

from ficha_etl import mirror


def test_item_id():
    assert mirror.item_id("2026-03") == "ficha-2026-03"


def test_item_id_rejects_bad_month():
    with pytest.raises(ValueError):
        mirror.item_id("not-a-month")


def test_raw_file_url():
    url = mirror.raw_file_url("2026-03", "Empresas0.zip")
    assert url == "https://archive.org/download/ficha-2026-03/raw/Empresas0.zip"


def test_parquet_url():
    url = mirror.parquet_url("2026-03", "cnpjs")
    assert url == "https://archive.org/download/ficha-2026-03/cnpjs.parquet"


def test_lookups_url():
    url = mirror.lookups_url("2026-03")
    assert url == "https://archive.org/download/ficha-2026-03/lookups.json"


def test_raw_files_for_month_count():
    files = mirror.raw_files_for_month("2026-03")
    assert len(files) == 37
    assert all(f.url.startswith("https://archive.org/download/ficha-2026-03/raw/") for f in files)


def test_base_url_env_override(monkeypatch):
    monkeypatch.setenv("FICHA_IA_BASE_URL", "https://example.test/dl")
    assert mirror.base_url() == "https://example.test/dl"
    assert mirror.raw_file_url("2026-03", "X.zip") == (
        "https://example.test/dl/ficha-2026-03/raw/X.zip"
    )


def test_health_url_default():
    assert mirror.health_url() == "https://archive.org/"


def test_health_url_env_override(monkeypatch):
    monkeypatch.setenv("FICHA_IA_HEALTH_URL", "https://example.test/")
    assert mirror.health_url() == "https://example.test/"
