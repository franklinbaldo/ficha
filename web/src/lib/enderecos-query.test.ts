import { describe, expect, it } from 'vitest';
import { normalizarLogradouro, planejarConsultaEnderecos } from './enderecos-query';

// Somente fixtures sintéticas: RFB 7107 e IBGE 3550308 representam sistemas
// distintos, NÃO uma correspondência afirmada entre municípios reais.
const fixture = {
  rfb: { sistema: 'RFB' as const, codigo: '7107' },
  ibge: { sistema: 'IBGE' as const, codigo: '3550308' },
  ruaPublicada: 'AVENIDA SAO JOAO',
};

describe('normalização do logradouro / paridade com a transformação', () => {
  it.each([
    ['Av.  São   João', 'AVENIDA SAO JOAO'],
    ['R. das Flores', 'RUA DAS FLORES'],
    ['Pça da Sé', 'PRACA DA SE'],
    ['Tv. da Paz', 'TRAVESSA DA PAZ'],
    ['Alameda das Águas', 'ALAMEDA DAS AGUAS'],
    ['AvEnIdA Brasil', 'AVENIDA BRASIL'],
  ])('normaliza %s em %s', (input, expected) => {
    expect(normalizarLogradouro(input)).toBe(expected);
  });

  it('reproduz falso negativo do filtro textual antigo e mostra paridade sintética', () => {
    const entrada = 'Av. São João';
    const filtroAntigo = entrada.toUpperCase();
    expect(fixture.ruaPublicada.includes(filtroAntigo)).toBe(false);
    expect(fixture.ruaPublicada.includes(normalizarLogradouro(entrada))).toBe(true);
  });
});

describe('planejamento de consulta local sem I/O', () => {
  it('delimita UF e município RFB, mantendo código e padrões como parâmetros', () => {
    const plano = planejarConsultaEnderecos({
      uf: 'sp', municipio: fixture.rfb, termo: 'Av. São João',
    });
    expect(plano.criterio).toBe('logradouro');
    expect(plano.parametros).toEqual(['SP', '7107', '%AVENIDA SAO JOAO%']);
    expect(plano.sql).toContain('uf = ? AND municipio_codigo = ? AND logradouro_normalizado ILIKE ?');
    expect(plano.sql).toContain('LIMIT 50');
    expect(plano.sql).not.toContain('São');
  });

  it('consulta município isolado, mas sempre com UF e código RFB', () => {
    const plano = planejarConsultaEnderecos({ uf: 'SP', municipio: fixture.rfb });
    expect(plano.criterio).toBe('municipio');
    expect(plano.parametros).toEqual(['SP', '7107']);
    expect(plano.sql).not.toContain('ILIKE');
  });

  it('preserva CEP como igualdade exata, sem aceitar wildcard', () => {
    const plano = planejarConsultaEnderecos({ uf: 'SP', termo: '01000-000' });
    expect(plano.criterio).toBe('cep');
    expect(plano.parametros).toEqual(['SP', '01000000']);
    expect(plano.sql).toContain('cep = ?');
    const wildcard = planejarConsultaEnderecos({ termo: 'Av%_ Sã\\o' });
    expect(wildcard.parametros).toEqual(['%AVENIDA SAO%']);
  });

  it('rejeita confusão IBGE/RFB e código de município de 7 dígitos', () => {
    expect(() => planejarConsultaEnderecos({ uf: 'SP', municipio: fixture.ibge }))
      .toThrow(/IBGE/);
    expect(() => planejarConsultaEnderecos({
      uf: 'SP', municipio: { sistema: 'RFB', codigo: '3550308' },
    })).toThrow(/RFB inválido/);
    expect(() => planejarConsultaEnderecos({ municipio: fixture.rfb }))
      .toThrow(/UF/);
  });

  it('rejeita consulta sem limite semântico e UF inválida', () => {
    expect(() => planejarConsultaEnderecos({})).toThrow();
    expect(() => planejarConsultaEnderecos({ termo: 'Rua', uf: 'SP;DROP' })).toThrow(/UF/);
    expect(() => planejarConsultaEnderecos({ termo: 'R' })).toThrow(/3 caracteres/);
  });
});
