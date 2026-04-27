from ficha_etl import upstream


def test_default_base_url():
    assert upstream.DEFAULT_RFB_BASE_URL == "https://dadosabertos.rfb.gov.br/CNPJ"


def test_base_url_env_override(monkeypatch):
    monkeypatch.setenv("FICHA_RFB_BASE_URL", "https://example.test/cnpj")
    assert upstream.base_url() == "https://example.test/cnpj"


def test_file_url():
    assert (
        upstream.file_url("Empresas0.zip")
        == "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip"
    )


def test_current_files_count_and_urls():
    files = upstream.current_files()
    assert len(files) == 37
    assert all(f.url.startswith("https://dadosabertos.rfb.gov.br/CNPJ/") for f in files)
    names = {f.name for f in files}
    assert "Empresas3.zip" in names
    assert "Cnaes.zip" in names
