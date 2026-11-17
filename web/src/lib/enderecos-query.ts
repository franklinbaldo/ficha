/**
 * Plano de consulta offline para a projeção `enderecos.parquet` (ADR 0023).
 * Não executa SQL nem acessa a rede. O componente de busca ainda precisa
 * integrar este plano após a estabilização da PR #246.
 *
 * `municipio_codigo` no Parquet é o código da RFB (4 dígitos), NÃO o código
 * IBGE (7 dígitos). Conversão entre catálogos exige tabela de correspondência.
 */
export type Municipio = { sistema: 'RFB' | 'IBGE'; codigo: string };
export type FiltroEnderecos = {
  termo?: string;
  uf?: string;
  municipio?: Municipio;
};
export type ConsultaEnderecos = {
  sql: string;
  parametros: string[];
  criterio: 'cep' | 'logradouro' | 'municipio';
};

const ABREVIACOES: Readonly<Record<string, string>> = {
  R: 'RUA',
  AV: 'AVENIDA',
  TV: 'TRAVESSA',
  AL: 'ALAMEDA',
  PCA: 'PRACA',
  PC: 'PRACA',
  EST: 'ESTRADA',
  ROD: 'RODOVIA',
  VL: 'VILA',
  LG: 'LARGO',
};

/** Espelha a ordem das transformações do writer: espaços, acentos, caixa e prefixo. */
export function normalizarLogradouro(entrada: string): string {
  const base = entrada.trim().replace(/\s+/gu, ' ')
    .normalize('NFD').replace(/\p{M}/gu, '').toUpperCase();
  const prefixo = /^([A-Z]+)\.?\s+/.exec(base);
  if (!prefixo) return base;
  const expandido = ABREVIACOES[prefixo[1] ?? ''];
  return expandido ? `${expandido} ${base.slice(prefixo[0].length)}` : base;
}

/**
 * Constrói SQL parametrizado, preservando o filtro de texto por substring.
 * Sem UF/município, uma consulta textual ainda pode varrer o arquivo grande:
 * esta função não promete pruning nem melhora de latência nesses casos.
 */
export function planejarConsultaEnderecos(filtro: FiltroEnderecos): ConsultaEnderecos {
  const condicoes: string[] = [];
  const parametros: string[] = [];
  const uf = filtro.uf?.trim().toUpperCase();
  if (uf) {
    if (!/^[A-Z]{2}$/.test(uf)) throw new Error('UF inválida');
    condicoes.push('uf = ?');
    parametros.push(uf);
  }
  if (filtro.municipio) {
    const { sistema, codigo } = filtro.municipio;
    if (sistema !== 'RFB') throw new Error('Código IBGE não é código RFB; exige correspondência explícita');
    if (!/^\d{4}$/.test(codigo)) throw new Error('Código de município RFB inválido');
    if (!uf) throw new Error('Informe UF para delimitar a consulta municipal');
    condicoes.push('municipio_codigo = ?');
    parametros.push(codigo);
  }

  // Parâmetros não são interpolados em SQL. Remove metacaracteres de LIKE
  // conforme a interface existente; nunca permite wildcard fornecido pelo usuário.
  const termo = (filtro.termo ?? '').trim().replace(/[%_\\]/g, '');
  let criterio: ConsultaEnderecos['criterio'];
  if (/^\d{5}-?\d{3}$/.test(termo)) {
    criterio = 'cep';
    condicoes.push('cep = ?');
    parametros.push(termo.replace('-', ''));
  } else if (termo) {
    const normalizado = normalizarLogradouro(termo);
    if (normalizado.length < 3) throw new Error('Logradouro requer pelo menos 3 caracteres');
    criterio = 'logradouro';
    condicoes.push('logradouro_normalizado ILIKE ?');
    parametros.push(`%${normalizado}%`);
  } else if (filtro.municipio) {
    criterio = 'municipio';
  } else {
    throw new Error('Informe logradouro, CEP ou município RFB');
  }

  const sql = `SELECT cnpj, uf, municipio_codigo, logradouro_normalizado, numero, bairro, cep\n` +
    `FROM enderecos WHERE ${condicoes.join(' AND ')}\n` +
    'ORDER BY uf, municipio_codigo, logradouro_normalizado, numero, cnpj\nLIMIT 50';
  return { sql, parametros, criterio };
}
