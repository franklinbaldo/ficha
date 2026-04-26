import os
import json
import zipfile
import ibis
import pyarrow as pa
import pyarrow.csv as pv
from pathlib import Path

# Configurações para a PoC de Amostragem Aleatória (Sampling) com IBIS
DATA_DIR = Path("/workspace/ficha/etl/data_sample")
OUTPUT_DIR = Path("/workspace/ficha/etl/output_poc")

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def generate_raw_csvs():
    """Gera arquivos CSV brutos para simular a RFB"""
    print("Gerando CSVs de teste...")
    
    # Estabelecimentos
    est_file = DATA_DIR / "ESTABELE.csv"
    with open(est_file, "w", encoding="iso-8859-1") as f:
        f.write("12345678;0001;91;1;FUNES IA;;;;;;;6201501;;;;;;;;AC;123\n")
        f.write("87654321;0001;00;1;HERMES LOG;;;;;;;4930202;;;;;;;;SP;456\n")
        f.write("11223344;0001;55;1;VERNE LAB;;;;;;;7210000;;;;;;;;SC;789\n")

    # Empresas
    emp_file = DATA_DIR / "EMPRESA.csv"
    with open(emp_file, "w", encoding="iso-8859-1") as f:
        f.write("12345678;FUNES MEMORIOSO LTDA;2062;05;100000,00;01;\n")
        f.write("87654321;HERMES TRANSPORTES SA;2054;08;5000000,00;05;\n")
        f.write("11223344;AGENCIA VERNE DE EXPLORACAO;2062;05;1500,00;01;\n")

def run_ibis_etl():
    print("Iniciando ETL com Ibis e PyArrow (para contornar bugs de encoding)...")
    
    # Conecta ao DuckDB
    con = ibis.duckdb.connect()
    
    # Usar PyArrow para ler o CSV com encoding ISO-8859-1 de forma robusta
    def read_csv_pyarrow(path):
        return pv.read_csv(
            path,
            read_options=pv.ReadOptions(autogenerate_column_names=True, encoding="iso-8859-1"),
            parse_options=pv.ParseOptions(delimiter=";"),
        )

    est_arrow = read_csv_pyarrow(DATA_DIR / "ESTABELE.csv")
    emp_arrow = read_csv_pyarrow(DATA_DIR / "EMPRESA.csv")
    
    # Criar tabelas Ibis a partir do Arrow
    estabelecimentos = ibis.memtable(est_arrow).rename({
        "cnpj_basico": "f0",
        "cnpj_ordem": "f1",
        "cnpj_dv": "f2",
        "nome_fantasia": "f4",
        "cnae_principal": "f11",
        "uf": "f19"
    })
    
    empresas = ibis.memtable(emp_arrow).rename({
        "cnpj_basico": "f0",
        "razao_social": "f1",
        "capital_social": "f4"
    })
    
    # Transformação
    est_full = estabelecimentos.mutate(
        cnpj = estabelecimentos.cnpj_basico + estabelecimentos.cnpj_ordem + estabelecimentos.cnpj_dv
    )
    
    ficha_analitica = est_full.join(empresas, "cnpj_basico")[
        "cnpj", "nome_fantasia", "cnae_principal", "uf", "razao_social", "capital_social"
    ]
    
    # Exportação Parquet
    parquet_path = OUTPUT_DIR / "base_amostra.parquet"
    ficha_analitica.to_parquet(parquet_path)
    print(f"Parquet gerado: {parquet_path}")
    
    # Exportação JSONs
    print("Gerando Camada Atômica...")
    results = ficha_analitica.execute()
    
    zip_path = OUTPUT_DIR / "fichario_amostra.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for _, row in results.iterrows():
            data = row.to_dict()
            cnpj = data['cnpj']
            json_content = json.dumps(data, indent=2, ensure_ascii=False)
            zf.writestr(f"{cnpj}.json", json_content)
            
    print(f"ZIP do Fichário gerado: {zip_path}")

if __name__ == "__main__":
    generate_raw_csvs()
    run_ibis_etl()
    print("PoC com Ibis, UV e PyArrow concluída!")
