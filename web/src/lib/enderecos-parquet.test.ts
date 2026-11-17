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

      // Fronteira de cobertura: reproduz o predicado atual do writer em
      // transform.py. Um logradouro somente com espaços ainda entra no
      // Parquet, enquanto NULL e string vazia não entram. Sem municipio
      // a linha também entra, mas não é recuperável pelo filtro municipal.
      // Isto documenta limites conhecidos, NÃO executa nem altera o ETL.
      const cobertura = conn.query(`
        WITH fonte(uf, municipio, logradouro) AS (
          VALUES
            ('SP', '7107', 'Av. São João'),
            ('SP', '7107', NULL),
            ('SP', '7107', ''),
            ('SP', '7107', '   '),
            (NULL, '7107', 'Rua Rio'),
            ('', '7107', 'Rua Rio'),
            ('SP', NULL, 'Rua Rio')
        ), elegiveis AS (
          SELECT * FROM fonte
          WHERE logradouro IS NOT NULL AND logradouro <> ''
            AND uf IS NOT NULL AND uf <> ''
        )
        SELECT COUNT(*) AS n,
          COUNT(*) FILTER (WHERE TRIM(logradouro) = '') AS so_espacos,
          COUNT(*) FILTER (WHERE municipio IS NULL) AS sem_municipio
        FROM elegiveis
      `).toArray()[0]?.toJSON() as {
        n: number | bigint; so_espacos: number | bigint;
        sem_municipio: number | bigint;
      };
      expect(Number(cobertura.n)).toBe(3);
      expect(Number(cobertura.so_espacos)).toBe(1);
      expect(Number(cobertura.sem_municipio)).toBe(1);

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

  it('preserva CNPJ alfanumérico como VARCHAR de 14 posições e isola municípios RFB', async () => {
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
      // Espelha a concatenação do writer transform.py, partindo de três
      // campos VARCHAR separados. CNPJs/DVs são sintéticos, não registros RFB.
      conn.query(`
        CREATE TABLE estabelecimento (
          cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR,
          uf VARCHAR, municipio VARCHAR, logradouro_normalizado VARCHAR,
          numero VARCHAR, bairro VARCHAR, cep VARCHAR
        )
      `);
      conn.query(`
        INSERT INTO estabelecimento VALUES
          ('00AB12C3', '0001', '91', 'SP', '7107', 'RUA DAS FLORES', '10', 'CENTRO', '01000000'),
          ('00AB12C3', '0002', '76', 'SP', '7108', 'RUA DAS FLORES', '11', 'CENTRO', '01000001'),
          ('00AB12C3', '0003', '50', 'RJ', '7107', 'RUA DAS FLORES', '12', 'CENTRO', '22000000'),
          ('00000001', '0001', '91', 'SP', '7107', 'RUA DAS FLORES', '13', 'CENTRO', '01000002')
      `);
      conn.query(`
        COPY (
          SELECT est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
                 est.uf, est.municipio AS municipio_codigo,
                 est.logradouro_normalizado, est.numero, est.bairro, est.cep
          FROM estabelecimento est
          ORDER BY est.uf, est.municipio, est.numero
        ) TO 'p3b-alfa.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
      `);
      const bytes = db.copyFileToBuffer('p3b-alfa.parquet').byteLength;
      expect(bytes).toBeGreaterThan(0);
      expect(bytes).toBeLessThan(100_000);
      const tipados = conn.query(
        "SELECT cnpj, typeof(cnpj) AS tipo FROM read_parquet('p3b-alfa.parquet') ORDER BY cnpj",
      ).toArray().map((row) => row.toJSON() as { cnpj: string; tipo: string });
      expect(tipados.map(({ cnpj }) => cnpj)).toEqual([
        '00000001000191', '00AB12C3000191', '00AB12C3000276', '00AB12C3000350',
      ]);
      expect(tipados.every(({ cnpj, tipo }) => tipo === 'VARCHAR' && cnpj.length === 14))
        .toBe(true);

      conn.query("CREATE VIEW enderecos AS SELECT * FROM 'p3b-alfa.parquet'");
      const consultar = (uf: string, codigo: string): string[] => {
        const plano = planejarConsultaEnderecos({
          uf, municipio: { sistema: 'RFB', codigo }, termo: 'Rua das Flores',
        });
        const stmt = conn.prepare(plano.sql);
        try {
          return stmt.query(...plano.parametros).toArray()
            .map((row) => row.toJSON().cnpj as string);
        } finally {
          stmt.close();
        }
      };
      expect(consultar('SP', '7107')).toEqual(['00AB12C3000191', '00000001000191']);
      expect(consultar('SP', '7108')).toEqual(['00AB12C3000276']);
      expect(consultar('RJ', '7107')).toEqual(['00AB12C3000350']);
      expect(consultar('RJ', '7108')).toEqual([]);
      expect(() => planejarConsultaEnderecos({
        uf: 'SP', municipio: { sistema: 'IBGE', codigo: '3550308' },
      })).toThrow(/IBGE/);
      expect(() => planejarConsultaEnderecos({
        uf: 'SP', municipio: { sistema: 'RFB', codigo: '3550308' },
      })).toThrow(/RFB inválido/);
    } finally {
      conn.close();
    }
  });
});
