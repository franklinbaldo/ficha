// Generated from knowledge/ by scripts/generate-okf-contracts.sh. Do not edit.
export type OkfConvenienceView = {
  readonly name: string;
  readonly inputs: readonly string[];
  readonly output: string;
  readonly purpose: string | null;
  readonly sql: string;
};

export const okfConvenienceViews = [
  {
    "name": "empresas_ativas",
    "inputs": [
      "Cnpj"
    ],
    "output": "Cnpj",
    "purpose": "Filtrar estabelecimentos cuja situação cadastral publicada é ativa.",
    "sql": "SELECT *\nFROM cnpjs\nWHERE situacao_cadastral = '02'"
  },
  {
    "name": "socios_de_raizes_ativas",
    "inputs": [
      "Socio",
      "Raiz"
    ],
    "output": "Socio",
    "purpose": "Filtrar vínculos societários de raízes com pelo menos um estabelecimento ativo.",
    "sql": "SELECT s.*\nFROM socios AS s\nJOIN raizes AS r USING (cnpj_base)\nWHERE r.qtd_estabelecimentos_ativos > 0"
  }
] as const satisfies readonly OkfConvenienceView[];
