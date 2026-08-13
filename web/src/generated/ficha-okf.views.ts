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
  }
] as const satisfies readonly OkfConvenienceView[];
