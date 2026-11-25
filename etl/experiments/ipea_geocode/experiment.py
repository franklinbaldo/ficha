"""Experimento offline-first de normalizacao e geocodificacao Ipea para Ficha.

CSV de entrada: tipo_logradouro,logradouro,numero,cep,bairro,municipio,uf.
A geocodificacao requer --geocode explicito e pode baixar dados CNEFE.
Nao modifica ETL, manifesto nem snapshots publicados.
"""
import argparse
import json
from pathlib import Path

from cohort import ficha_key, read_cohort


def run(path, *, geocode=False):
    result = read_cohort(path)
    selected = [r for r in result["records"] if r["query_eligible"]]
    if geocode and not selected:
        result["geocode_status"] = "empty_cohort"
    if geocode and selected:
        try:
            import polars as pl
            from geocodebr import definir_campos, geocode as geocode_fn
        except ImportError as error:
            raise RuntimeError("Install geocodebr and polars separately") from error
        rows = [r["original_fields"] for r in selected]
        addresses = pl.DataFrame({
            "_ficha_row_id": [r["row_id"] for r in selected],
            "logradouro": [r["query_street"] for r in selected],
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
        result["queried_rows"] = len(selected)
        result["geocode_identity_status"] = "unverified"
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
        # Exclusive creation rejects existing paths, including hardlinks and symlinks.
        try:
            with args.output.open("x", encoding="utf-8") as handle:
                handle.write(output)
        except FileExistsError:
            parser.error("--output must be a new file; existing paths are not overwritten")
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
