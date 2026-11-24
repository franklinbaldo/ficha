"""Experimento offline-first de normalizacao e geocodificacao Ipea para Ficha.

CSV de entrada: tipo_logradouro,logradouro,numero,cep,bairro,municipio,uf.
A geocodificacao requer --geocode explicito e pode baixar dados CNEFE.
Nao modifica ETL, manifesto nem snapshots publicados.
"""
import argparse
import csv
import json
import re
import unicodedata
from collections import Counter
from pathlib import Path

PREFIX = {"R": "RUA", "AV": "AVENIDA", "TV": "TRAVESSA", "AL": "ALAMEDA",
          "PCA": "PRACA", "PC": "PRACA", "EST": "ESTRADA", "ROD": "RODOVIA",
          "VL": "VILA", "LG": "LARGO"}
KNOWN = set(PREFIX) | set(PREFIX.values())


def folding(value):
    value = unicodedata.normalize("NFKD", str(value or "").strip())
    return re.sub(r"\s+", " ", "".join(
        c for c in value if not unicodedata.combining(c)
    ).upper()).strip()


def ficha_key(row):
    street = folding(row["logradouro"])
    first = re.match(r"^([A-Z]+)\.?\s+", street)
    if not first or first.group(1) not in KNOWN:
        kind = folding(row.get("tipo_logradouro"))
        street = f"{kind} {street}".strip() if kind else street
    match = re.match(r"^([A-Z]+)\.?\s+(.*)$", street)
    if match and match.group(1) in PREFIX:
        return f"{PREFIX[match.group(1)]} {match.group(2)}"
    return street


def run(path, *, geocode=False):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"tipo_logradouro", "logradouro", "numero", "cep",
                    "bairro", "municipio", "uf"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"Missing CSV columns: {', '.join(sorted(missing))}")
        rows = list(reader)
    if not rows:
        raise ValueError("Empty CSV")
    keys = [ficha_key(row) for row in rows]
    groups = Counter(
        (folding(row["uf"]), folding(row["municipio"]), keys[index])
        for index, row in enumerate(rows)
    )
    result = {
        "input_rows": len(rows),
        "unique_street_groups": len(groups),
        "colliding_street_groups": sum(value > 1 for value in groups.values()),
        "geocode_status": "not_run",
        "records": [
            {"row": index, "id": row.get("id", str(index)),
             "original": row["logradouro"], "ficha_normalizado": keys[index]}
            for index, row in enumerate(rows)
        ],
    }
    if geocode:
        try:
            import polars as pl
            from geocodebr import definir_campos, geocode as geocode_fn
        except ImportError as error:
            raise RuntimeError("Install geocodebr and polars separately") from error
        addresses = pl.DataFrame({
            "logradouro": [
                f'{row.get("tipo_logradouro", "")} {row["logradouro"]}'.strip()
                for row in rows
            ],
            "numero": [row["numero"] for row in rows],
            "cep": [row["cep"] for row in rows],
            "localidade": [row["bairro"] for row in rows],
            "municipio": [row["municipio"] for row in rows],
            "estado": [row["uf"] for row in rows],
        })
        fields = definir_campos(
            logradouro="logradouro", numero="numero", cep="cep",
            localidade="localidade", municipio="municipio", estado="estado",
        )
        table = geocode_fn(
            enderecos=addresses, campos_endereco=fields, resultado_gpd=False
        )
        records = (
            table.to_pylist() if hasattr(table, "to_pylist")
            else table.to_dicts() if hasattr(table, "to_dicts")
            else table.to_dict("records")
        )
        result["geocode_status"] = "completed"
        result["geocode_output_rows"] = len(records)
        result["geocode_output_columns"] = list(records[0]) if records else []
        # Nao associar resultados pela posicao: o geocoder pode reordenar.
        result["geocode_unjoined"] = records
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--geocode", action="store_true",
                        help="May download CNEFE cache, approximately 1.2 GB")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = json.dumps(run(args.input_csv, geocode=args.geocode),
                        ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
