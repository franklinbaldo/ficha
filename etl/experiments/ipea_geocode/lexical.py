"""Opt-in lexical comparison; never imports geocodebr or downloads CNEFE.

Linux x86_64 wheel: enderecobr==0.3.0, SHA256
845c5e45a8cbc5ebcb0475a69aba91d04b182586eaec6976aef80d352d69f3a2.
Install separately from PyPI with --only-binary=:all: --no-deps.
"""

import argparse
import json
from collections import defaultdict
from importlib.metadata import version
from pathlib import Path

from cohort import folding, read_cohort

VERSION = "0.3.0"


def compare(report, normalize):
    rows = []
    baseline_groups = defaultdict(set)
    ipea_groups = defaultdict(set)
    for record in report["records"]:
        if not record["query_eligible"]:
            continue
        fields = record["original_fields"]
        region = (folding(fields["uf"]), folding(fields["municipio"]))
        baseline = record["ficha_normalizado"]
        result = normalize(record["query_street"])
        if not isinstance(result, str):
            raise TypeError("Expected string from lexical normalizer")
        baseline_groups[(*region, baseline)].add(result)
        ipea_groups[(*region, result)].add(baseline)
        rows.append({**record, "ipea_normalizado": result, "equal": baseline == result})
    return {
        "input_sha256": report["input_sha256"],
        "input_rows": report["input_rows"],
        "compared_rows": len(rows),
        "equal_rows": sum(row["equal"] for row in rows),
        "different_rows": sum(not row["equal"] for row in rows),
        "empty_ipea_rows": sum(not row["ipea_normalizado"].strip() for row in rows),
        "baseline_split_groups": sum(len(values) > 1 for values in baseline_groups.values()),
        "ipea_merged_groups": sum(len(values) > 1 for values in ipea_groups.values()),
        "records": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path)
    args = parser.parse_args()
    if version("enderecobr") != VERSION:
        parser.error(f"Install enderecobr=={VERSION} in an isolated environment")
    from enderecobr import padronizar_logradouros

    result = compare(read_cohort(args.input_csv), padronizar_logradouros)
    result["enderecobr_version"] = VERSION
    result["geocode_status"] = "not_run"
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
