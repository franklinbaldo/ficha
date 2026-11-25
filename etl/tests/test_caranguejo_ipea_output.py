"""Exercise the experimental CLI without network or production ETL."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "experiments/ipea_geocode/experiment.py"
CSV = (
    "tipo_logradouro,logradouro,numero,cep,bairro,municipio,uf\n"
    "AV,São João,10,00000000,Centro,Porto Velho,RO\n"
).encode()


def invoke(source, output, optimized):
    flags = ["-O"] if optimized else []
    return subprocess.run(
        [sys.executable, *flags, str(SCRIPT), str(source), "--output", str(output)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )


@pytest.mark.parametrize("optimized", [False, True])
@pytest.mark.parametrize("alias", ["same", "relative", "hardlink", "symlink", "existing", "new"])
def test_report_never_overwrites(tmp_path, optimized, alias):
    source = tmp_path / "input.csv"
    source.write_bytes(CSV)
    output = tmp_path / "report.json"
    if alias == "same":
        output = source
    elif alias == "relative":
        sub = tmp_path / "sub"
        sub.mkdir()
        output = sub / ".." / source.name
    elif alias in {"hardlink", "symlink"}:
        try:
            if alias == "hardlink":
                os.link(source, output)
            else:
                output.symlink_to(source)
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"{alias} unavailable: {exc}")
    elif alias == "existing":
        output.write_bytes(b"existing report")

    result = invoke(source, output, optimized)
    assert source.read_bytes() == CSV
    if alias == "new":
        assert result.returncode == 0, result.stderr
        report = json.loads(output.read_text(encoding="utf-8"))
        assert report["geocode_status"] == "not_run"
        assert report["records"][0]["ficha_normalizado"] == "AVENIDA SAO JOAO"
    else:
        assert result.returncode == 2, result.stderr
        assert "--output must be a new file" in result.stderr
        expected = b"existing report" if alias == "existing" else CSV
        assert output.read_bytes() == expected
        if alias == "symlink":
            assert output.is_symlink()


@pytest.mark.parametrize("optimized", [False, True])
def test_experiment_suite_is_collected(optimized):
    flags = ["-O"] if optimized else []
    result = subprocess.run(
        [sys.executable, *flags, "-m", "unittest", "-v", "test_experiment.py", "test_cohort.py"],
        cwd=SCRIPT.parent,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "Ran 11 tests" in result.stderr
