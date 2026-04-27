import os
import json
import zipfile
import duckdb
import pandas as pd
from pathlib import Path

# Configurações para a PoC do Acre (AC)
STATE = "AC"
DATA_DIR = Path("/workspace/ficha/etl/poc_acre/data")
OUTPUT_DIR = Path("/workspace/ficha/etl/poc_acre/output")
JSON_DIR = OUTPUT_DIR / "jsons"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
JSON_DIR.mkdir(parents=True, exist_ok=True)

def mock_data_generation():
    """
    Como o download da RFB pode ser demorado e instável, 
    para esta PoC inicial vamos gerar um subset de dados fakes 
    seguindo o layout real da RFB para testar o pipeline.
    """
    print("Gerando dados de exemplo (PoC)...")
    
    # Exemplo simplificado de Estabelecimentos
    estabelecimentos = [
        {"cnpj_basico": "12345678", "cnpj_ordem": "0001", "cnpj_dv": "91", "uf": "AC", "nome_fantasia": "PADARIA DO ACRE"},
        {"cnpj_basico": "87654321", "cnpj_ordem": "0001", "cnpj_dv": "00", "uf": "AC", "nome_fantasia": "BORRACHARIA RIO BRANCO"},
    ]
    
    # Exemplo simplificado de Empresas
    empresas = [
        {"cnpj_basico": "12345678", "razao_social": "JOSE DA SILVA ALIMENTOS LTDA", "capital_social": 50000.00},
        {"cnpj_basico": "87654321", "razao_social": "MARIA SOUZA MANUTENCAO ME", "capital_social": 10000.00},
    ]
    
    # Exemplo simplificado de Sócios
    socios = [
        {"cnpj_basico": "12345678", "nome_socio": "JOSE DA SILVA", "qualificacao": "Sócio-Administrador"},
        {"cnpj_basico": "87654321", "nome_socio": "MARIA SOUZA", "qualificacao": "Titular Pessoa Física"},
    ]
    
    df_est = pd.DataFrame(estabelecimentos)
    df_emp = pd.DataFrame(empresas)
    df_soc = pd.DataFrame(socios)
    
    # Salva CSVs temporários simulando os da RFB
    df_est.to_csv(DATA_DIR / "estabelecimentos.csv", index=False)
    df_emp.to_csv(DATA_DIR / "empresas.csv", index=False)
    df_soc.to_csv(DATA_DIR / "socios.csv", index=False)

def run_etl():
    print("Iniciando ETL com DuckDB...")
    con = duckdb.connect()
    
    # 1. Carrega e faz JOIN (Camada Analítica)
    con.execute(f"""
        CREATE TABLE ficha_consolidada AS 
        SELECT 
            e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj,
            e.*, 
            emp.razao_social, 
            emp.capital_social
        FROM '{DATA_DIR}/estabelecimentos.csv' e
        JOIN '{DATA_DIR}/empresas.csv' emp ON e.cnpj_basico = emp.cnpj_basico
    """)
    
    # 2. Exporta Parquet
    parquet_path = OUTPUT_DIR / f"cnpjs_{STATE}.parquet"
    con.execute(f"COPY ficha_consolidada TO '{parquet_path}' (FORMAT PARQUET)")
    print(f"Parquet gerado em: {parquet_path}")
    
    # 3. Gera JSONs Individuais (Camada Atômica)
    print("Gerando JSONs atômicos...")
    results = con.execute("SELECT * FROM ficha_consolidada").fetchall()
    columns = [desc[0] for desc in con.description]
    
    zip_path = OUTPUT_DIR / f"cnpjs_{STATE}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for row in results:
            data = dict(zip(columns, row))
            cnpj = data['cnpj']
            json_content = json.dumps(data, indent=2, ensure_ascii=False)
            zf.writestr(f"{cnpj}.json", json_content)
            
    print(f"ZIP com JSONs atômicos gerado em: {zip_path}")

if __name__ == "__main__":
    mock_data_generation()
    run_etl()
    print("PoC concluída com sucesso!")
