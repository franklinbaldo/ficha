import * as $protobuf from "protobufjs";
import Long = require("long");

/** Namespace ficha. */
export namespace ficha {

    /** Namespace v1. */
    namespace v1 {

        /** Porte enum. */
        enum Porte {

            /** PORTE_UNSPECIFIED value */
            PORTE_UNSPECIFIED = 0,

            /** NAO_INFORMADO value */
            NAO_INFORMADO = 1,

            /** MICRO_EMPRESA value */
            MICRO_EMPRESA = 2,

            /** PEQUENO_PORTE value */
            PEQUENO_PORTE = 3,

            /** DEMAIS value */
            DEMAIS = 5
        }

        /** TipoEstabelecimento enum. */
        enum TipoEstabelecimento {

            /** TIPO_ESTAB_UNSPECIFIED value */
            TIPO_ESTAB_UNSPECIFIED = 0,

            /** MATRIZ value */
            MATRIZ = 1,

            /** FILIAL value */
            FILIAL = 2
        }

        /** TipoSocio enum. */
        enum TipoSocio {

            /** TIPO_SOCIO_UNSPECIFIED value */
            TIPO_SOCIO_UNSPECIFIED = 0,

            /** PESSOA_JURIDICA value */
            PESSOA_JURIDICA = 1,

            /** PESSOA_FISICA value */
            PESSOA_FISICA = 2,

            /** ESTRANGEIRO value */
            ESTRANGEIRO = 3
        }

        /** FaixaEtaria enum. */
        enum FaixaEtaria {

            /** FAIXA_ETARIA_UNSPECIFIED value */
            FAIXA_ETARIA_UNSPECIFIED = 0,

            /** ATE_12 value */
            ATE_12 = 1,

            /** DE_13_A_20 value */
            DE_13_A_20 = 2,

            /** DE_21_A_30 value */
            DE_21_A_30 = 3,

            /** DE_31_A_40 value */
            DE_31_A_40 = 4,

            /** DE_41_A_50 value */
            DE_41_A_50 = 5,

            /** DE_51_A_60 value */
            DE_51_A_60 = 6,

            /** DE_61_A_70 value */
            DE_61_A_70 = 7,

            /** DE_71_A_80 value */
            DE_71_A_80 = 8,

            /** ACIMA_80 value */
            ACIMA_80 = 9,

            /** NAO_INFORMADA value */
            NAO_INFORMADA = 10
        }

        /**
         * Properties of a Company.
         * @deprecated Use ficha.v1.Company.$Properties instead.
         */
        interface ICompany extends ficha.v1.Company.$Properties {
        }

        /** Represents a Company. */
        class Company {

            /**
             * Constructs a new Company.
             * @param [properties] Properties to set
             */
            constructor(properties?: ficha.v1.Company.$Properties);

            /** Unknown fields preserved while decoding */
            $unknowns?: Uint8Array[];

            /** Company cnpj_base. */
            cnpj_base: number;

            /** Company razao_social. */
            razao_social: string;

            /** Company razao_social_normalizada. */
            razao_social_normalizada: string;

            /** Company natureza_juridica_codigo. */
            natureza_juridica_codigo: number;

            /** Company porte_empresa. */
            porte_empresa: ficha.v1.Porte;

            /** Company capital_social. */
            capital_social: number;

            /** Company ente_federativo_responsavel. */
            ente_federativo_responsavel: string;

            /** Company qtd_estabelecimentos. */
            qtd_estabelecimentos: number;

            /** Company qtd_estabelecimentos_ativos. */
            qtd_estabelecimentos_ativos: number;

            /** Company estabelecimentos. */
            estabelecimentos: ficha.v1.Estabelecimento.$Properties[];

            /** Company socios. */
            socios: ficha.v1.Socio.$Properties[];

            /** Company snapshot_yyyymm. */
            snapshot_yyyymm: number;

            /**
             * Creates a new Company instance using the specified properties.
             * @param [properties] Properties to set
             * @returns Company instance
             */
            static create(properties: ficha.v1.Company.$Shape): ficha.v1.Company & ficha.v1.Company.$Shape;
            static create(properties?: ficha.v1.Company.$Properties): ficha.v1.Company;

            /**
             * Encodes the specified Company message. Does not implicitly {@link ficha.v1.Company.verify|verify} messages.
             * @param message Company message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encode(message: ficha.v1.Company.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified Company message, length delimited. Does not implicitly {@link ficha.v1.Company.verify|verify} messages.
             * @param message Company message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encodeDelimited(message: ficha.v1.Company.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a Company message from the specified reader or buffer.
             * @param reader Reader or buffer to decode from
             * @param [length] Message length if known beforehand
             * @returns {ficha.v1.Company & ficha.v1.Company.$Shape} Company
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ficha.v1.Company & ficha.v1.Company.$Shape;

            /**
             * Decodes a Company message from the specified reader or buffer, length delimited.
             * @param reader Reader or buffer to decode from
             * @returns {ficha.v1.Company & ficha.v1.Company.$Shape} Company
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ficha.v1.Company & ficha.v1.Company.$Shape;

            /**
             * Verifies a Company message.
             * @param message Plain object to verify
             * @returns `null` if valid, otherwise the reason why it is not
             */
            static verify(message: { [k: string]: any }): (string|null);

            /**
             * Creates a Company message from a plain object. Also converts values to their respective internal types.
             * @param object Plain object
             * @returns Company
             */
            static fromObject(object: { [k: string]: any }): ficha.v1.Company;

            /**
             * Creates a plain object from a Company message. Also converts values to other types if specified.
             * @param message Company
             * @param [options] Conversion options
             * @returns Plain object
             */
            static toObject(message: ficha.v1.Company, options?: $protobuf.IConversionOptions): { [k: string]: any };

            /**
             * Converts this Company to JSON.
             * @returns JSON object
             */
            toJSON(): { [k: string]: any };

            /**
             * Gets the type url for Company
             * @param [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns The type url
             */
            static getTypeUrl(prefix?: string): string;
        }

        namespace Company {

            /** Properties of a Company. */
            interface $Properties {

                /** Company cnpj_base */
                cnpj_base?: (number|null);

                /** Company razao_social */
                razao_social?: (string|null);

                /** Company razao_social_normalizada */
                razao_social_normalizada?: (string|null);

                /** Company natureza_juridica_codigo */
                natureza_juridica_codigo?: (number|null);

                /** Company porte_empresa */
                porte_empresa?: (ficha.v1.Porte|null);

                /** Company capital_social */
                capital_social?: (number|null);

                /** Company ente_federativo_responsavel */
                ente_federativo_responsavel?: (string|null);

                /** Company qtd_estabelecimentos */
                qtd_estabelecimentos?: (number|null);

                /** Company qtd_estabelecimentos_ativos */
                qtd_estabelecimentos_ativos?: (number|null);

                /** Company estabelecimentos */
                estabelecimentos?: (ficha.v1.Estabelecimento.$Properties[]|null);

                /** Company socios */
                socios?: (ficha.v1.Socio.$Properties[]|null);

                /** Company snapshot_yyyymm */
                snapshot_yyyymm?: (number|null);

                /** Unknown fields preserved while decoding */
                $unknowns?: Uint8Array[];
            }

            /** Shape of a Company. */
            type $Shape = ficha.v1.Company.$Properties;
        }

        /**
         * Properties of an Estabelecimento.
         * @deprecated Use ficha.v1.Estabelecimento.$Properties instead.
         */
        interface IEstabelecimento extends ficha.v1.Estabelecimento.$Properties {
        }

        /** Represents an Estabelecimento. */
        class Estabelecimento {

            /**
             * Constructs a new Estabelecimento.
             * @param [properties] Properties to set
             */
            constructor(properties?: ficha.v1.Estabelecimento.$Properties);

            /** Unknown fields preserved while decoding */
            $unknowns?: Uint8Array[];

            /** Estabelecimento cnpj_ordem. */
            cnpj_ordem: number;

            /** Estabelecimento cnpj_dv. */
            cnpj_dv: number;

            /** Estabelecimento tipo. */
            tipo: ficha.v1.TipoEstabelecimento;

            /** Estabelecimento nome_fantasia. */
            nome_fantasia: string;

            /** Estabelecimento situacao_cadastral. */
            situacao_cadastral: number;

            /** Estabelecimento data_situacao_cadastral. */
            data_situacao_cadastral: number;

            /** Estabelecimento motivo_situacao_cadastral_codigo. */
            motivo_situacao_cadastral_codigo: number;

            /** Estabelecimento situacao_especial. */
            situacao_especial: string;

            /** Estabelecimento data_situacao_especial. */
            data_situacao_especial: number;

            /** Estabelecimento data_inicio_atividade. */
            data_inicio_atividade: number;

            /** Estabelecimento cnae_principal_codigo. */
            cnae_principal_codigo: number;

            /** Estabelecimento cnaes_secundarios_codigos. */
            cnaes_secundarios_codigos: number[];

            /** Estabelecimento tipo_logradouro. */
            tipo_logradouro: string;

            /** Estabelecimento logradouro. */
            logradouro: string;

            /** Estabelecimento numero. */
            numero: string;

            /** Estabelecimento complemento. */
            complemento: string;

            /** Estabelecimento bairro. */
            bairro: string;

            /** Estabelecimento cep. */
            cep: number;

            /** Estabelecimento uf. */
            uf: string;

            /** Estabelecimento municipio_codigo. */
            municipio_codigo: number;

            /** Estabelecimento nome_cidade_exterior. */
            nome_cidade_exterior: string;

            /** Estabelecimento pais_codigo. */
            pais_codigo: number;

            /** Estabelecimento ddd_1. */
            ddd_1: string;

            /** Estabelecimento telefone_1. */
            telefone_1: string;

            /** Estabelecimento ddd_2. */
            ddd_2: string;

            /** Estabelecimento telefone_2. */
            telefone_2: string;

            /** Estabelecimento ddd_fax. */
            ddd_fax: string;

            /** Estabelecimento fax. */
            fax: string;

            /** Estabelecimento correio_eletronico. */
            correio_eletronico: string;

            /** Estabelecimento opcao_simples. */
            opcao_simples: boolean;

            /** Estabelecimento data_opcao_simples. */
            data_opcao_simples: number;

            /** Estabelecimento data_exclusao_simples. */
            data_exclusao_simples: number;

            /** Estabelecimento opcao_mei. */
            opcao_mei: boolean;

            /** Estabelecimento data_opcao_mei. */
            data_opcao_mei: number;

            /** Estabelecimento data_exclusao_mei. */
            data_exclusao_mei: number;

            /**
             * Creates a new Estabelecimento instance using the specified properties.
             * @param [properties] Properties to set
             * @returns Estabelecimento instance
             */
            static create(properties: ficha.v1.Estabelecimento.$Shape): ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape;
            static create(properties?: ficha.v1.Estabelecimento.$Properties): ficha.v1.Estabelecimento;

            /**
             * Encodes the specified Estabelecimento message. Does not implicitly {@link ficha.v1.Estabelecimento.verify|verify} messages.
             * @param message Estabelecimento message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encode(message: ficha.v1.Estabelecimento.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified Estabelecimento message, length delimited. Does not implicitly {@link ficha.v1.Estabelecimento.verify|verify} messages.
             * @param message Estabelecimento message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encodeDelimited(message: ficha.v1.Estabelecimento.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an Estabelecimento message from the specified reader or buffer.
             * @param reader Reader or buffer to decode from
             * @param [length] Message length if known beforehand
             * @returns {ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape} Estabelecimento
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape;

            /**
             * Decodes an Estabelecimento message from the specified reader or buffer, length delimited.
             * @param reader Reader or buffer to decode from
             * @returns {ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape} Estabelecimento
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape;

            /**
             * Verifies an Estabelecimento message.
             * @param message Plain object to verify
             * @returns `null` if valid, otherwise the reason why it is not
             */
            static verify(message: { [k: string]: any }): (string|null);

            /**
             * Creates an Estabelecimento message from a plain object. Also converts values to their respective internal types.
             * @param object Plain object
             * @returns Estabelecimento
             */
            static fromObject(object: { [k: string]: any }): ficha.v1.Estabelecimento;

            /**
             * Creates a plain object from an Estabelecimento message. Also converts values to other types if specified.
             * @param message Estabelecimento
             * @param [options] Conversion options
             * @returns Plain object
             */
            static toObject(message: ficha.v1.Estabelecimento, options?: $protobuf.IConversionOptions): { [k: string]: any };

            /**
             * Converts this Estabelecimento to JSON.
             * @returns JSON object
             */
            toJSON(): { [k: string]: any };

            /**
             * Gets the type url for Estabelecimento
             * @param [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns The type url
             */
            static getTypeUrl(prefix?: string): string;
        }

        namespace Estabelecimento {

            /** Properties of an Estabelecimento. */
            interface $Properties {

                /** Estabelecimento cnpj_ordem */
                cnpj_ordem?: (number|null);

                /** Estabelecimento cnpj_dv */
                cnpj_dv?: (number|null);

                /** Estabelecimento tipo */
                tipo?: (ficha.v1.TipoEstabelecimento|null);

                /** Estabelecimento nome_fantasia */
                nome_fantasia?: (string|null);

                /** Estabelecimento situacao_cadastral */
                situacao_cadastral?: (number|null);

                /** Estabelecimento data_situacao_cadastral */
                data_situacao_cadastral?: (number|null);

                /** Estabelecimento motivo_situacao_cadastral_codigo */
                motivo_situacao_cadastral_codigo?: (number|null);

                /** Estabelecimento situacao_especial */
                situacao_especial?: (string|null);

                /** Estabelecimento data_situacao_especial */
                data_situacao_especial?: (number|null);

                /** Estabelecimento data_inicio_atividade */
                data_inicio_atividade?: (number|null);

                /** Estabelecimento cnae_principal_codigo */
                cnae_principal_codigo?: (number|null);

                /** Estabelecimento cnaes_secundarios_codigos */
                cnaes_secundarios_codigos?: (number[]|null);

                /** Estabelecimento tipo_logradouro */
                tipo_logradouro?: (string|null);

                /** Estabelecimento logradouro */
                logradouro?: (string|null);

                /** Estabelecimento numero */
                numero?: (string|null);

                /** Estabelecimento complemento */
                complemento?: (string|null);

                /** Estabelecimento bairro */
                bairro?: (string|null);

                /** Estabelecimento cep */
                cep?: (number|null);

                /** Estabelecimento uf */
                uf?: (string|null);

                /** Estabelecimento municipio_codigo */
                municipio_codigo?: (number|null);

                /** Estabelecimento nome_cidade_exterior */
                nome_cidade_exterior?: (string|null);

                /** Estabelecimento pais_codigo */
                pais_codigo?: (number|null);

                /** Estabelecimento ddd_1 */
                ddd_1?: (string|null);

                /** Estabelecimento telefone_1 */
                telefone_1?: (string|null);

                /** Estabelecimento ddd_2 */
                ddd_2?: (string|null);

                /** Estabelecimento telefone_2 */
                telefone_2?: (string|null);

                /** Estabelecimento ddd_fax */
                ddd_fax?: (string|null);

                /** Estabelecimento fax */
                fax?: (string|null);

                /** Estabelecimento correio_eletronico */
                correio_eletronico?: (string|null);

                /** Estabelecimento opcao_simples */
                opcao_simples?: (boolean|null);

                /** Estabelecimento data_opcao_simples */
                data_opcao_simples?: (number|null);

                /** Estabelecimento data_exclusao_simples */
                data_exclusao_simples?: (number|null);

                /** Estabelecimento opcao_mei */
                opcao_mei?: (boolean|null);

                /** Estabelecimento data_opcao_mei */
                data_opcao_mei?: (number|null);

                /** Estabelecimento data_exclusao_mei */
                data_exclusao_mei?: (number|null);

                /** Unknown fields preserved while decoding */
                $unknowns?: Uint8Array[];
            }

            /** Shape of an Estabelecimento. */
            type $Shape = ficha.v1.Estabelecimento.$Properties;
        }

        /**
         * Properties of a Socio.
         * @deprecated Use ficha.v1.Socio.$Properties instead.
         */
        interface ISocio extends ficha.v1.Socio.$Properties {
        }

        /** Represents a Socio. */
        class Socio {

            /**
             * Constructs a new Socio.
             * @param [properties] Properties to set
             */
            constructor(properties?: ficha.v1.Socio.$Properties);

            /** Unknown fields preserved while decoding */
            $unknowns?: Uint8Array[];

            /** Socio tipo. */
            tipo: ficha.v1.TipoSocio;

            /** Socio nome_socio_razao_social. */
            nome_socio_razao_social: string;

            /** Socio cpf_mascarado_meio. */
            cpf_mascarado_meio: number;

            /** Socio cnpj_socio. */
            cnpj_socio: number;

            /** Socio qualificacao_codigo. */
            qualificacao_codigo: number;

            /** Socio data_entrada_sociedade. */
            data_entrada_sociedade: number;

            /** Socio pais_codigo. */
            pais_codigo: number;

            /** Socio faixa_etaria. */
            faixa_etaria: ficha.v1.FaixaEtaria;

            /** Socio representante_legal_cpf_meio. */
            representante_legal_cpf_meio: number;

            /** Socio representante_legal_nome. */
            representante_legal_nome: string;

            /** Socio representante_legal_qualificacao_codigo. */
            representante_legal_qualificacao_codigo: number;

            /**
             * Creates a new Socio instance using the specified properties.
             * @param [properties] Properties to set
             * @returns Socio instance
             */
            static create(properties: ficha.v1.Socio.$Shape): ficha.v1.Socio & ficha.v1.Socio.$Shape;
            static create(properties?: ficha.v1.Socio.$Properties): ficha.v1.Socio;

            /**
             * Encodes the specified Socio message. Does not implicitly {@link ficha.v1.Socio.verify|verify} messages.
             * @param message Socio message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encode(message: ficha.v1.Socio.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified Socio message, length delimited. Does not implicitly {@link ficha.v1.Socio.verify|verify} messages.
             * @param message Socio message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encodeDelimited(message: ficha.v1.Socio.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a Socio message from the specified reader or buffer.
             * @param reader Reader or buffer to decode from
             * @param [length] Message length if known beforehand
             * @returns {ficha.v1.Socio & ficha.v1.Socio.$Shape} Socio
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ficha.v1.Socio & ficha.v1.Socio.$Shape;

            /**
             * Decodes a Socio message from the specified reader or buffer, length delimited.
             * @param reader Reader or buffer to decode from
             * @returns {ficha.v1.Socio & ficha.v1.Socio.$Shape} Socio
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ficha.v1.Socio & ficha.v1.Socio.$Shape;

            /**
             * Verifies a Socio message.
             * @param message Plain object to verify
             * @returns `null` if valid, otherwise the reason why it is not
             */
            static verify(message: { [k: string]: any }): (string|null);

            /**
             * Creates a Socio message from a plain object. Also converts values to their respective internal types.
             * @param object Plain object
             * @returns Socio
             */
            static fromObject(object: { [k: string]: any }): ficha.v1.Socio;

            /**
             * Creates a plain object from a Socio message. Also converts values to other types if specified.
             * @param message Socio
             * @param [options] Conversion options
             * @returns Plain object
             */
            static toObject(message: ficha.v1.Socio, options?: $protobuf.IConversionOptions): { [k: string]: any };

            /**
             * Converts this Socio to JSON.
             * @returns JSON object
             */
            toJSON(): { [k: string]: any };

            /**
             * Gets the type url for Socio
             * @param [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns The type url
             */
            static getTypeUrl(prefix?: string): string;
        }

        namespace Socio {

            /** Properties of a Socio. */
            interface $Properties {

                /** Socio tipo */
                tipo?: (ficha.v1.TipoSocio|null);

                /** Socio nome_socio_razao_social */
                nome_socio_razao_social?: (string|null);

                /** Socio cpf_mascarado_meio */
                cpf_mascarado_meio?: (number|null);

                /** Socio cnpj_socio */
                cnpj_socio?: (number|null);

                /** Socio qualificacao_codigo */
                qualificacao_codigo?: (number|null);

                /** Socio data_entrada_sociedade */
                data_entrada_sociedade?: (number|null);

                /** Socio pais_codigo */
                pais_codigo?: (number|null);

                /** Socio faixa_etaria */
                faixa_etaria?: (ficha.v1.FaixaEtaria|null);

                /** Socio representante_legal_cpf_meio */
                representante_legal_cpf_meio?: (number|null);

                /** Socio representante_legal_nome */
                representante_legal_nome?: (string|null);

                /** Socio representante_legal_qualificacao_codigo */
                representante_legal_qualificacao_codigo?: (number|null);

                /** Unknown fields preserved while decoding */
                $unknowns?: Uint8Array[];
            }

            /** Shape of a Socio. */
            type $Shape = ficha.v1.Socio.$Properties;
        }

        /**
         * Properties of a LookupEntry.
         * @deprecated Use ficha.v1.LookupEntry.$Properties instead.
         */
        interface ILookupEntry extends ficha.v1.LookupEntry.$Properties {
        }

        /** Represents a LookupEntry. */
        class LookupEntry {

            /**
             * Constructs a new LookupEntry.
             * @param [properties] Properties to set
             */
            constructor(properties?: ficha.v1.LookupEntry.$Properties);

            /** Unknown fields preserved while decoding */
            $unknowns?: Uint8Array[];

            /** LookupEntry codigo. */
            codigo: number;

            /** LookupEntry descricao. */
            descricao: string;

            /**
             * Creates a new LookupEntry instance using the specified properties.
             * @param [properties] Properties to set
             * @returns LookupEntry instance
             */
            static create(properties: ficha.v1.LookupEntry.$Shape): ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape;
            static create(properties?: ficha.v1.LookupEntry.$Properties): ficha.v1.LookupEntry;

            /**
             * Encodes the specified LookupEntry message. Does not implicitly {@link ficha.v1.LookupEntry.verify|verify} messages.
             * @param message LookupEntry message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encode(message: ficha.v1.LookupEntry.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified LookupEntry message, length delimited. Does not implicitly {@link ficha.v1.LookupEntry.verify|verify} messages.
             * @param message LookupEntry message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encodeDelimited(message: ficha.v1.LookupEntry.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a LookupEntry message from the specified reader or buffer.
             * @param reader Reader or buffer to decode from
             * @param [length] Message length if known beforehand
             * @returns {ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape} LookupEntry
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape;

            /**
             * Decodes a LookupEntry message from the specified reader or buffer, length delimited.
             * @param reader Reader or buffer to decode from
             * @returns {ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape} LookupEntry
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape;

            /**
             * Verifies a LookupEntry message.
             * @param message Plain object to verify
             * @returns `null` if valid, otherwise the reason why it is not
             */
            static verify(message: { [k: string]: any }): (string|null);

            /**
             * Creates a LookupEntry message from a plain object. Also converts values to their respective internal types.
             * @param object Plain object
             * @returns LookupEntry
             */
            static fromObject(object: { [k: string]: any }): ficha.v1.LookupEntry;

            /**
             * Creates a plain object from a LookupEntry message. Also converts values to other types if specified.
             * @param message LookupEntry
             * @param [options] Conversion options
             * @returns Plain object
             */
            static toObject(message: ficha.v1.LookupEntry, options?: $protobuf.IConversionOptions): { [k: string]: any };

            /**
             * Converts this LookupEntry to JSON.
             * @returns JSON object
             */
            toJSON(): { [k: string]: any };

            /**
             * Gets the type url for LookupEntry
             * @param [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns The type url
             */
            static getTypeUrl(prefix?: string): string;
        }

        namespace LookupEntry {

            /** Properties of a LookupEntry. */
            interface $Properties {

                /** LookupEntry codigo */
                codigo?: (number|null);

                /** LookupEntry descricao */
                descricao?: (string|null);

                /** Unknown fields preserved while decoding */
                $unknowns?: Uint8Array[];
            }

            /** Shape of a LookupEntry. */
            type $Shape = ficha.v1.LookupEntry.$Properties;
        }

        /**
         * Properties of a LookupFile.
         * @deprecated Use ficha.v1.LookupFile.$Properties instead.
         */
        interface ILookupFile extends ficha.v1.LookupFile.$Properties {
        }

        /** Represents a LookupFile. */
        class LookupFile {

            /**
             * Constructs a new LookupFile.
             * @param [properties] Properties to set
             */
            constructor(properties?: ficha.v1.LookupFile.$Properties);

            /** Unknown fields preserved while decoding */
            $unknowns?: Uint8Array[];

            /** LookupFile kind. */
            kind: string;

            /** LookupFile entries. */
            entries: ficha.v1.LookupEntry.$Properties[];

            /**
             * Creates a new LookupFile instance using the specified properties.
             * @param [properties] Properties to set
             * @returns LookupFile instance
             */
            static create(properties: ficha.v1.LookupFile.$Shape): ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape;
            static create(properties?: ficha.v1.LookupFile.$Properties): ficha.v1.LookupFile;

            /**
             * Encodes the specified LookupFile message. Does not implicitly {@link ficha.v1.LookupFile.verify|verify} messages.
             * @param message LookupFile message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encode(message: ficha.v1.LookupFile.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified LookupFile message, length delimited. Does not implicitly {@link ficha.v1.LookupFile.verify|verify} messages.
             * @param message LookupFile message or plain object to encode
             * @param [writer] Writer to encode to
             * @returns Writer
             */
            static encodeDelimited(message: ficha.v1.LookupFile.$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a LookupFile message from the specified reader or buffer.
             * @param reader Reader or buffer to decode from
             * @param [length] Message length if known beforehand
             * @returns {ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape} LookupFile
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape;

            /**
             * Decodes a LookupFile message from the specified reader or buffer, length delimited.
             * @param reader Reader or buffer to decode from
             * @returns {ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape} LookupFile
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape;

            /**
             * Verifies a LookupFile message.
             * @param message Plain object to verify
             * @returns `null` if valid, otherwise the reason why it is not
             */
            static verify(message: { [k: string]: any }): (string|null);

            /**
             * Creates a LookupFile message from a plain object. Also converts values to their respective internal types.
             * @param object Plain object
             * @returns LookupFile
             */
            static fromObject(object: { [k: string]: any }): ficha.v1.LookupFile;

            /**
             * Creates a plain object from a LookupFile message. Also converts values to other types if specified.
             * @param message LookupFile
             * @param [options] Conversion options
             * @returns Plain object
             */
            static toObject(message: ficha.v1.LookupFile, options?: $protobuf.IConversionOptions): { [k: string]: any };

            /**
             * Converts this LookupFile to JSON.
             * @returns JSON object
             */
            toJSON(): { [k: string]: any };

            /**
             * Gets the type url for LookupFile
             * @param [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns The type url
             */
            static getTypeUrl(prefix?: string): string;
        }

        namespace LookupFile {

            /** Properties of a LookupFile. */
            interface $Properties {

                /** LookupFile kind */
                kind?: (string|null);

                /** LookupFile entries */
                entries?: (ficha.v1.LookupEntry.$Properties[]|null);

                /** Unknown fields preserved while decoding */
                $unknowns?: Uint8Array[];
            }

            /** Shape of a LookupFile. */
            type $Shape = ficha.v1.LookupFile.$Properties;
        }
    }
}
