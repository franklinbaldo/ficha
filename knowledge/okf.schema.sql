CREATE TABLE "Raiz" (
    cnpj_base VARCHAR PRIMARY KEY
);

CREATE TABLE "Cnpj" (
    cnpj VARCHAR PRIMARY KEY,
    cnpj_base VARCHAR REFERENCES "Raiz"(cnpj_base)
);

CREATE TABLE "Socio" (
    cnpj_base VARCHAR REFERENCES "Raiz"(cnpj_base)
);
