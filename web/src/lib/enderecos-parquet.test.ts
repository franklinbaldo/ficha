import { createRequire } from 'node:module';
import { describe, expect, it } from 'vitest';
import {
  normalizarLogradouro,
  planejarConsultaEnderecos,
  type FiltroEnderecos,
} from './enderecos-query';

// Integração local com o MESMO DuckDB-WASM usado pelo site, sem rede/IA/ETL.
// Cada execução constrói um Parquet temporário de quatro linhas sintéticas.
const require = createRequire(import.meta.url);

describe('P3-B: execução real de SQL parametrizado sobre Parquet sintético', () => {
  it('separa UF/RFB, normaliza o logradouro e consulta CEP sem falsos positivos', async () => {
    const duckdb = require('@duckdb/duckdb-wasm/blocking') as
      typeof import('@duckdb/duckdb-wasm/blocking');
    const db = await duckdb.createDuckDB(
      {
        mvp: {
          mainModule: require.resolve('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm'),
          mainWorker: require.resolve('@duckdb/duckdb-wasm/dist/duckdb-node-mvp.worker.cjs'),
        },
      },
      new duckdb.VoidLogger(),
      duckdb.NODE_RUNTIME,
    );
    await db.instantiate();
    db.open({ path: ':memory:' });
    const conn = db.connect();

    try {
      conn.query(`
        CREATE TABLE fonte_enderecos (
          cnpj VARCHAR, uf VARCHAR, municipio_codigo VARCHAR,
          logradouro_normalizado VARCHAR, numero VARCHAR, bairro VARCHAR, cep VARCHAR
        )
      `);
      conn.query(`
        INSERT INTO fonte_enderecos VALUES
          ('00000001000191', 'SP', '7107', 'AVENIDA SAO JOAO', '12', 'CENTRO', '01000000'),
          ('00000002000176', 'SP', '7108', 'RUA DAS FLORES', '5', 'JARDIM', '01000001'),
          ('00000003000150', 'RJ', '7107', 'AVENIDA SAO JOAO', '1', 'CENTRO', '22000000'),
          ('00000004000135', 'SP', '7107', 'RUA DAS FLORES', '9', 'CENTRO', '01000002')
      `);
      conn.query(`
        COPY (
          SELECT * FROM fonte_enderecos
          ORDER BY uf, municipio_codigo, logradouro_normalizado, numero
        ) TO 'p3b-fixture.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
      `);
      const bytes = db.copyFileToBuffer('p3b-fixture.parquet').byteLength;
      expect(bytes).toBeGreaterThan(0);
      expect(bytes).toBeLessThan(100_000); // guarda contra fixture onerosa
      const contagem = conn.query(
        "SELECT COUNT(*) AS n FROM read_parquet('p3b-fixture.parquet')",
      ).toArray()[0]?.toJSON().n as number | bigint;
      expect(Number(contagem)).toBe(4);
      conn.query("CREATE VIEW enderecos AS SELECT * FROM 'p3b-fixture.parquet'");


      // Testa a expressão SQL efetiva do writer em entradas cruas sintéticas,
      // evitando afirmar paridade apenas porque a fixture já veio normalizada.
      // Espelho de transform.py/write_enderecos_parquet (sem executar ETL real).
      const linhasNormalizadas = conn.query(String.raw`
        WITH entradas(logradouro) AS (
          VALUES ('R. das Flores'), ('Av.  São   João'), ('TV. da Paz'),
                 ('AL. das Águas'), ('Pça da Sé'), ('PC. das Artes'),
                 ('EST. Linha 1'), ('ROD. BR-364'), ('VL. Nova'),
                 ('LG. da Feira'), ('Avenida Brasil')
        ), _base AS (
          SELECT logradouro,
                 UPPER(strip_accents(TRIM(
                   regexp_replace(logradouro, '\s+', ' ', 'g')
                 ))) AS _logr
          FROM entradas
        )
        SELECT logradouro,
               COALESCE(
                 MAP {
                   'R': 'RUA ', 'AV': 'AVENIDA ', 'TV': 'TRAVESSA ',
                   'AL': 'ALAMEDA ', 'PCA': 'PRACA ', 'PC': 'PRACA ',
                   'EST': 'ESTRADA ', 'ROD': 'RODOVIA ',
                   'VL': 'VILA ', 'LG': 'LARGO '
                 }[regexp_extract(_logr, '^([A-Z]+)\.?\s+', 1)]
                   || regexp_replace(_logr, '^[A-Z]+\.?\s+', ''),
                 _logr
               ) AS logradouro_normalizado
        FROM _base ORDER BY logradouro
      `).toArray().map((linha) => linha.toJSON() as {
        logradouro: string; logradouro_normalizado: string;
      });
      expect(linhasNormalizadas).toHaveLength(11);
      for (const linha of linhasNormalizadas) {
        expect(normalizarLogradouro(linha.logradouro))
          .toBe(linha.logradouro_normalizado);
      }

      const consultar = (filtro: FiltroEnderecos): string[] => {
        const plano = planejarConsultaEnderecos(filtro);
        const stmt = conn.prepare(plano.sql);
        try {
          return stmt.query(...plano.parametros).toArray()
            .map((row) => row.toJSON().cnpj as string);
        } finally {
          stmt.close();
        }
      };
      const rfb = { sistema: 'RFB' as const, codigo: '7107' };
      expect(consultar({ uf: 'SP', municipio: rfb, termo: 'Av. São João' }))
        .toEqual(['00000001000191']);
      expect(consultar({ uf: 'RJ', municipio: rfb, termo: 'Av. São João' }))
        .toEqual(['00000003000150']);
      expect(consultar({ uf: 'SP', municipio: rfb, termo: '01000-000' }))
        .toEqual(['00000001000191']);
      expect(consultar({ uf: 'SP', municipio: rfb }))
        .toEqual(['00000001000191', '00000004000135']);
      expect(consultar({ uf: 'SP', municipio: rfb, termo: 'Rua das Flores' }))
        .toEqual(['00000004000135']);
      expect(consultar({ uf: 'SP', municipio: rfb, termo: 'Rua%_Inexistente' }))
        .toEqual([]);

      // Falso negativo reproduzido em SQL real pela ausência de normalização antiga.
      const antigo = conn.prepare(
        'SELECT cnpj FROM enderecos WHERE uf = ? AND logradouro_normalizado ILIKE ?',
      );
      try {
        expect(antigo.query('SP', '%AV. SÃO JOÃO%').toArray()).toHaveLength(0);
      } finally {
        antigo.close();
      }
      expect(() => consultar({
        uf: 'SP', municipio: { sistema: 'IBGE', codigo: '3550308' },
      })).toThrow(/IBGE/);
    } finally {
      conn.close();
    }
  });
});
