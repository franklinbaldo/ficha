/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars, default-case, jsdoc/require-param*/
import $protobuf from "protobufjs/minimal.js";

// Common aliases
const $Reader = $protobuf.Reader, $Writer = $protobuf.Writer, $util = $protobuf.util;

// Exported root namespace
const $root = $protobuf.roots["default"] || ($protobuf.roots["default"] = {});

export const ficha = $root.ficha = (() => {

    /**
     * Namespace ficha.
     * @exports ficha
     * @namespace
     */
    const ficha = {};

    ficha.v1 = (function() {

        /**
         * Namespace v1.
         * @memberof ficha
         * @namespace
         */
        const v1 = {};

        /**
         * Porte enum.
         * @name ficha.v1.Porte
         * @enum {number}
         * @property {number} PORTE_UNSPECIFIED=0 PORTE_UNSPECIFIED value
         * @property {number} NAO_INFORMADO=1 NAO_INFORMADO value
         * @property {number} MICRO_EMPRESA=2 MICRO_EMPRESA value
         * @property {number} PEQUENO_PORTE=3 PEQUENO_PORTE value
         * @property {number} DEMAIS=5 DEMAIS value
         */
        v1.Porte = (function() {
            const valuesById = {}, values = Object.create(valuesById);
            values[valuesById[0] = "PORTE_UNSPECIFIED"] = 0;
            values[valuesById[1] = "NAO_INFORMADO"] = 1;
            values[valuesById[2] = "MICRO_EMPRESA"] = 2;
            values[valuesById[3] = "PEQUENO_PORTE"] = 3;
            values[valuesById[5] = "DEMAIS"] = 5;
            return values;
        })();

        /**
         * TipoEstabelecimento enum.
         * @name ficha.v1.TipoEstabelecimento
         * @enum {number}
         * @property {number} TIPO_ESTAB_UNSPECIFIED=0 TIPO_ESTAB_UNSPECIFIED value
         * @property {number} MATRIZ=1 MATRIZ value
         * @property {number} FILIAL=2 FILIAL value
         */
        v1.TipoEstabelecimento = (function() {
            const valuesById = {}, values = Object.create(valuesById);
            values[valuesById[0] = "TIPO_ESTAB_UNSPECIFIED"] = 0;
            values[valuesById[1] = "MATRIZ"] = 1;
            values[valuesById[2] = "FILIAL"] = 2;
            return values;
        })();

        /**
         * TipoSocio enum.
         * @name ficha.v1.TipoSocio
         * @enum {number}
         * @property {number} TIPO_SOCIO_UNSPECIFIED=0 TIPO_SOCIO_UNSPECIFIED value
         * @property {number} PESSOA_JURIDICA=1 PESSOA_JURIDICA value
         * @property {number} PESSOA_FISICA=2 PESSOA_FISICA value
         * @property {number} ESTRANGEIRO=3 ESTRANGEIRO value
         */
        v1.TipoSocio = (function() {
            const valuesById = {}, values = Object.create(valuesById);
            values[valuesById[0] = "TIPO_SOCIO_UNSPECIFIED"] = 0;
            values[valuesById[1] = "PESSOA_JURIDICA"] = 1;
            values[valuesById[2] = "PESSOA_FISICA"] = 2;
            values[valuesById[3] = "ESTRANGEIRO"] = 3;
            return values;
        })();

        /**
         * FaixaEtaria enum.
         * @name ficha.v1.FaixaEtaria
         * @enum {number}
         * @property {number} FAIXA_ETARIA_UNSPECIFIED=0 FAIXA_ETARIA_UNSPECIFIED value
         * @property {number} ATE_12=1 ATE_12 value
         * @property {number} DE_13_A_20=2 DE_13_A_20 value
         * @property {number} DE_21_A_30=3 DE_21_A_30 value
         * @property {number} DE_31_A_40=4 DE_31_A_40 value
         * @property {number} DE_41_A_50=5 DE_41_A_50 value
         * @property {number} DE_51_A_60=6 DE_51_A_60 value
         * @property {number} DE_61_A_70=7 DE_61_A_70 value
         * @property {number} DE_71_A_80=8 DE_71_A_80 value
         * @property {number} ACIMA_80=9 ACIMA_80 value
         * @property {number} NAO_INFORMADA=10 NAO_INFORMADA value
         */
        v1.FaixaEtaria = (function() {
            const valuesById = {}, values = Object.create(valuesById);
            values[valuesById[0] = "FAIXA_ETARIA_UNSPECIFIED"] = 0;
            values[valuesById[1] = "ATE_12"] = 1;
            values[valuesById[2] = "DE_13_A_20"] = 2;
            values[valuesById[3] = "DE_21_A_30"] = 3;
            values[valuesById[4] = "DE_31_A_40"] = 4;
            values[valuesById[5] = "DE_41_A_50"] = 5;
            values[valuesById[6] = "DE_51_A_60"] = 6;
            values[valuesById[7] = "DE_61_A_70"] = 7;
            values[valuesById[8] = "DE_71_A_80"] = 8;
            values[valuesById[9] = "ACIMA_80"] = 9;
            values[valuesById[10] = "NAO_INFORMADA"] = 10;
            return values;
        })();

        v1.Company = (function() {

            /**
             * Properties of a Company.
             * @typedef {Object} ficha.v1.Company.$Properties
             * @property {number|null} [cnpj_base] Company cnpj_base
             * @property {string|null} [razao_social] Company razao_social
             * @property {string|null} [razao_social_normalizada] Company razao_social_normalizada
             * @property {number|null} [natureza_juridica_codigo] Company natureza_juridica_codigo
             * @property {ficha.v1.Porte|null} [porte_empresa] Company porte_empresa
             * @property {number|null} [capital_social] Company capital_social
             * @property {string|null} [ente_federativo_responsavel] Company ente_federativo_responsavel
             * @property {number|null} [qtd_estabelecimentos] Company qtd_estabelecimentos
             * @property {number|null} [qtd_estabelecimentos_ativos] Company qtd_estabelecimentos_ativos
             * @property {Array.<ficha.v1.Estabelecimento.$Properties>|null} [estabelecimentos] Company estabelecimentos
             * @property {Array.<ficha.v1.Socio.$Properties>|null} [socios] Company socios
             * @property {number|null} [snapshot_yyyymm] Company snapshot_yyyymm
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */

            /**
             * Properties of a Company.
             * @memberof ficha.v1
             * @interface ICompany
             * @augments ficha.v1.Company.$Properties
             * @deprecated Use ficha.v1.Company.$Properties instead.
             */

            /**
             * Shape of a Company.
             * @typedef {ficha.v1.Company.$Properties} ficha.v1.Company.$Shape
             */

            /**
             * Constructs a new Company.
             * @memberof ficha.v1
             * @classdesc Represents a Company.
             * @constructor
             * @param {ficha.v1.Company.$Properties=} [properties] Properties to set
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */
            function Company(properties) {
                this.estabelecimentos = [];
                this.socios = [];
                if (properties)
                    for (let keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null && keys[i] !== "__proto__")
                            this[keys[i]] = properties[keys[i]];
            }

            /**
             * Company cnpj_base.
             * @member {number} cnpj_base
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.cnpj_base = 0;

            /**
             * Company razao_social.
             * @member {string} razao_social
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.razao_social = "";

            /**
             * Company razao_social_normalizada.
             * @member {string} razao_social_normalizada
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.razao_social_normalizada = "";

            /**
             * Company natureza_juridica_codigo.
             * @member {number} natureza_juridica_codigo
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.natureza_juridica_codigo = 0;

            /**
             * Company porte_empresa.
             * @member {ficha.v1.Porte} porte_empresa
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.porte_empresa = 0;

            /**
             * Company capital_social.
             * @member {number} capital_social
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.capital_social = 0;

            /**
             * Company ente_federativo_responsavel.
             * @member {string} ente_federativo_responsavel
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.ente_federativo_responsavel = "";

            /**
             * Company qtd_estabelecimentos.
             * @member {number} qtd_estabelecimentos
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.qtd_estabelecimentos = 0;

            /**
             * Company qtd_estabelecimentos_ativos.
             * @member {number} qtd_estabelecimentos_ativos
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.qtd_estabelecimentos_ativos = 0;

            /**
             * Company estabelecimentos.
             * @member {Array.<ficha.v1.Estabelecimento.$Properties>} estabelecimentos
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.estabelecimentos = $util.emptyArray;

            /**
             * Company socios.
             * @member {Array.<ficha.v1.Socio.$Properties>} socios
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.socios = $util.emptyArray;

            /**
             * Company snapshot_yyyymm.
             * @member {number} snapshot_yyyymm
             * @memberof ficha.v1.Company
             * @instance
             */
            Company.prototype.snapshot_yyyymm = 0;

            /**
             * Creates a new Company instance using the specified properties.
             * @function create
             * @memberof ficha.v1.Company
             * @static
             * @param {ficha.v1.Company.$Properties=} [properties] Properties to set
             * @returns {ficha.v1.Company} Company instance
             * @type {{
             *   (properties: ficha.v1.Company.$Shape): ficha.v1.Company & ficha.v1.Company.$Shape;
             *   (properties?: ficha.v1.Company.$Properties): ficha.v1.Company;
             * }}
             */
            Company.create = function create(properties) {
                return new Company(properties);
            };

            /**
             * Encodes the specified Company message. Does not implicitly {@link ficha.v1.Company.verify|verify} messages.
             * @function encode
             * @memberof ficha.v1.Company
             * @static
             * @param {ficha.v1.Company.$Properties} message Company message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Company.encode = function encode(message, writer) {
                if (!writer)
                    writer = $Writer.create();
                if (message.cnpj_base != null && Object.hasOwnProperty.call(message, "cnpj_base"))
                    writer.uint32(/* id 1, wireType 0 =*/8).uint32(message.cnpj_base);
                if (message.razao_social != null && Object.hasOwnProperty.call(message, "razao_social"))
                    writer.uint32(/* id 2, wireType 2 =*/18).string(message.razao_social);
                if (message.razao_social_normalizada != null && Object.hasOwnProperty.call(message, "razao_social_normalizada"))
                    writer.uint32(/* id 3, wireType 2 =*/26).string(message.razao_social_normalizada);
                if (message.natureza_juridica_codigo != null && Object.hasOwnProperty.call(message, "natureza_juridica_codigo"))
                    writer.uint32(/* id 10, wireType 0 =*/80).uint32(message.natureza_juridica_codigo);
                if (message.porte_empresa != null && Object.hasOwnProperty.call(message, "porte_empresa"))
                    writer.uint32(/* id 11, wireType 0 =*/88).int32(message.porte_empresa);
                if (message.capital_social != null && Object.hasOwnProperty.call(message, "capital_social"))
                    writer.uint32(/* id 12, wireType 1 =*/97).double(message.capital_social);
                if (message.ente_federativo_responsavel != null && Object.hasOwnProperty.call(message, "ente_federativo_responsavel"))
                    writer.uint32(/* id 13, wireType 2 =*/106).string(message.ente_federativo_responsavel);
                if (message.qtd_estabelecimentos != null && Object.hasOwnProperty.call(message, "qtd_estabelecimentos"))
                    writer.uint32(/* id 14, wireType 0 =*/112).uint32(message.qtd_estabelecimentos);
                if (message.qtd_estabelecimentos_ativos != null && Object.hasOwnProperty.call(message, "qtd_estabelecimentos_ativos"))
                    writer.uint32(/* id 15, wireType 0 =*/120).uint32(message.qtd_estabelecimentos_ativos);
                if (message.estabelecimentos != null && message.estabelecimentos.length)
                    for (let i = 0; i < message.estabelecimentos.length; ++i)
                        $root.ficha.v1.Estabelecimento.encode(message.estabelecimentos[i], writer.uint32(/* id 20, wireType 2 =*/162).fork()).ldelim();
                if (message.socios != null && message.socios.length)
                    for (let i = 0; i < message.socios.length; ++i)
                        $root.ficha.v1.Socio.encode(message.socios[i], writer.uint32(/* id 21, wireType 2 =*/170).fork()).ldelim();
                if (message.snapshot_yyyymm != null && Object.hasOwnProperty.call(message, "snapshot_yyyymm"))
                    writer.uint32(/* id 100, wireType 0 =*/800).uint32(message.snapshot_yyyymm);
                if (message.$unknowns != null && Object.hasOwnProperty.call(message, "$unknowns"))
                    for (let i = 0; i < message.$unknowns.length; ++i)
                        writer.raw(message.$unknowns[i]);
                return writer;
            };

            /**
             * Encodes the specified Company message, length delimited. Does not implicitly {@link ficha.v1.Company.verify|verify} messages.
             * @function encodeDelimited
             * @memberof ficha.v1.Company
             * @static
             * @param {ficha.v1.Company.$Properties} message Company message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Company.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a Company message from the specified reader or buffer.
             * @function decode
             * @memberof ficha.v1.Company
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {ficha.v1.Company & ficha.v1.Company.$Shape} Company
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Company.decode = function decode(reader, length, _end, _depth, _target) {
                if (!(reader instanceof $Reader))
                    reader = $Reader.create(reader);
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $Reader.recursionLimit)
                    throw Error("max depth exceeded");
                let end = length === undefined ? reader.len : reader.pos + length, message = _target || new $root.ficha.v1.Company(), value;
                while (reader.pos < end) {
                    let start = reader.pos;
                    let tag = reader.tag();
                    if (tag === _end) {
                        _end = undefined;
                        break;
                    }
                    let wireType = tag & 7;
                    switch (tag >>>= 3) {
                    case 1: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.cnpj_base = value;
                            else
                                delete message.cnpj_base;
                            continue;
                        }
                    case 2: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.razao_social = value;
                            else
                                delete message.razao_social;
                            continue;
                        }
                    case 3: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.razao_social_normalizada = value;
                            else
                                delete message.razao_social_normalizada;
                            continue;
                        }
                    case 10: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.natureza_juridica_codigo = value;
                            else
                                delete message.natureza_juridica_codigo;
                            continue;
                        }
                    case 11: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.int32())
                                message.porte_empresa = value;
                            else
                                delete message.porte_empresa;
                            continue;
                        }
                    case 12: {
                            if (wireType !== 1)
                                break;
                            if ((value = reader.double()) !== 0)
                                message.capital_social = value;
                            else
                                delete message.capital_social;
                            continue;
                        }
                    case 13: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.ente_federativo_responsavel = value;
                            else
                                delete message.ente_federativo_responsavel;
                            continue;
                        }
                    case 14: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.qtd_estabelecimentos = value;
                            else
                                delete message.qtd_estabelecimentos;
                            continue;
                        }
                    case 15: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.qtd_estabelecimentos_ativos = value;
                            else
                                delete message.qtd_estabelecimentos_ativos;
                            continue;
                        }
                    case 20: {
                            if (wireType !== 2)
                                break;
                            if (!(message.estabelecimentos && message.estabelecimentos.length))
                                message.estabelecimentos = [];
                            message.estabelecimentos.push($root.ficha.v1.Estabelecimento.decode(reader, reader.uint32(), undefined, _depth + 1));
                            continue;
                        }
                    case 21: {
                            if (wireType !== 2)
                                break;
                            if (!(message.socios && message.socios.length))
                                message.socios = [];
                            message.socios.push($root.ficha.v1.Socio.decode(reader, reader.uint32(), undefined, _depth + 1));
                            continue;
                        }
                    case 100: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.snapshot_yyyymm = value;
                            else
                                delete message.snapshot_yyyymm;
                            continue;
                        }
                    }
                    reader.skipType(wireType, _depth, tag);
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
                if (_end !== undefined)
                    throw Error("missing end group");
                return message;
            };

            /**
             * Decodes a Company message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof ficha.v1.Company
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {ficha.v1.Company & ficha.v1.Company.$Shape} Company
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Company.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader))
                    reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a Company message.
             * @function verify
             * @memberof ficha.v1.Company
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            Company.verify = function verify(message, _depth) {
                if (typeof message !== "object" || message === null)
                    return "object expected";
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    return "max depth exceeded";
                if (message.cnpj_base != null && message.hasOwnProperty("cnpj_base"))
                    if (!$util.isInteger(message.cnpj_base))
                        return "cnpj_base: integer expected";
                if (message.razao_social != null && message.hasOwnProperty("razao_social"))
                    if (!$util.isString(message.razao_social))
                        return "razao_social: string expected";
                if (message.razao_social_normalizada != null && message.hasOwnProperty("razao_social_normalizada"))
                    if (!$util.isString(message.razao_social_normalizada))
                        return "razao_social_normalizada: string expected";
                if (message.natureza_juridica_codigo != null && message.hasOwnProperty("natureza_juridica_codigo"))
                    if (!$util.isInteger(message.natureza_juridica_codigo))
                        return "natureza_juridica_codigo: integer expected";
                if (message.porte_empresa != null && message.hasOwnProperty("porte_empresa"))
                    switch (message.porte_empresa) {
                    default:
                        return "porte_empresa: enum value expected";
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 5:
                        break;
                    }
                if (message.capital_social != null && message.hasOwnProperty("capital_social"))
                    if (typeof message.capital_social !== "number")
                        return "capital_social: number expected";
                if (message.ente_federativo_responsavel != null && message.hasOwnProperty("ente_federativo_responsavel"))
                    if (!$util.isString(message.ente_federativo_responsavel))
                        return "ente_federativo_responsavel: string expected";
                if (message.qtd_estabelecimentos != null && message.hasOwnProperty("qtd_estabelecimentos"))
                    if (!$util.isInteger(message.qtd_estabelecimentos))
                        return "qtd_estabelecimentos: integer expected";
                if (message.qtd_estabelecimentos_ativos != null && message.hasOwnProperty("qtd_estabelecimentos_ativos"))
                    if (!$util.isInteger(message.qtd_estabelecimentos_ativos))
                        return "qtd_estabelecimentos_ativos: integer expected";
                if (message.estabelecimentos != null && message.hasOwnProperty("estabelecimentos")) {
                    if (!Array.isArray(message.estabelecimentos))
                        return "estabelecimentos: array expected";
                    for (let i = 0; i < message.estabelecimentos.length; ++i) {
                        let error = $root.ficha.v1.Estabelecimento.verify(message.estabelecimentos[i], _depth + 1);
                        if (error)
                            return "estabelecimentos." + error;
                    }
                }
                if (message.socios != null && message.hasOwnProperty("socios")) {
                    if (!Array.isArray(message.socios))
                        return "socios: array expected";
                    for (let i = 0; i < message.socios.length; ++i) {
                        let error = $root.ficha.v1.Socio.verify(message.socios[i], _depth + 1);
                        if (error)
                            return "socios." + error;
                    }
                }
                if (message.snapshot_yyyymm != null && message.hasOwnProperty("snapshot_yyyymm"))
                    if (!$util.isInteger(message.snapshot_yyyymm))
                        return "snapshot_yyyymm: integer expected";
                return null;
            };

            /**
             * Creates a Company message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof ficha.v1.Company
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {ficha.v1.Company} Company
             */
            Company.fromObject = function fromObject(object, _depth) {
                if (object instanceof $root.ficha.v1.Company)
                    return object;
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    throw Error("max depth exceeded");
                let message = new $root.ficha.v1.Company();
                if (object.cnpj_base != null)
                    if (Number(object.cnpj_base) !== 0)
                        message.cnpj_base = object.cnpj_base >>> 0;
                if (object.razao_social != null)
                    if (typeof object.razao_social !== "string" || object.razao_social.length)
                        message.razao_social = String(object.razao_social);
                if (object.razao_social_normalizada != null)
                    if (typeof object.razao_social_normalizada !== "string" || object.razao_social_normalizada.length)
                        message.razao_social_normalizada = String(object.razao_social_normalizada);
                if (object.natureza_juridica_codigo != null)
                    if (Number(object.natureza_juridica_codigo) !== 0)
                        message.natureza_juridica_codigo = object.natureza_juridica_codigo >>> 0;
                if (object.porte_empresa !== 0 && (typeof object.porte_empresa !== "string" || $root.ficha.v1.Porte[object.porte_empresa] !== 0))
                    switch (object.porte_empresa) {
                    default:
                        if (typeof object.porte_empresa === "number") {
                            message.porte_empresa = object.porte_empresa;
                            break;
                        }
                        break;
                    case "PORTE_UNSPECIFIED":
                    case 0:
                        message.porte_empresa = 0;
                        break;
                    case "NAO_INFORMADO":
                    case 1:
                        message.porte_empresa = 1;
                        break;
                    case "MICRO_EMPRESA":
                    case 2:
                        message.porte_empresa = 2;
                        break;
                    case "PEQUENO_PORTE":
                    case 3:
                        message.porte_empresa = 3;
                        break;
                    case "DEMAIS":
                    case 5:
                        message.porte_empresa = 5;
                        break;
                    }
                if (object.capital_social != null)
                    if (Number(object.capital_social) !== 0)
                        message.capital_social = Number(object.capital_social);
                if (object.ente_federativo_responsavel != null)
                    if (typeof object.ente_federativo_responsavel !== "string" || object.ente_federativo_responsavel.length)
                        message.ente_federativo_responsavel = String(object.ente_federativo_responsavel);
                if (object.qtd_estabelecimentos != null)
                    if (Number(object.qtd_estabelecimentos) !== 0)
                        message.qtd_estabelecimentos = object.qtd_estabelecimentos >>> 0;
                if (object.qtd_estabelecimentos_ativos != null)
                    if (Number(object.qtd_estabelecimentos_ativos) !== 0)
                        message.qtd_estabelecimentos_ativos = object.qtd_estabelecimentos_ativos >>> 0;
                if (object.estabelecimentos) {
                    if (!Array.isArray(object.estabelecimentos))
                        throw TypeError(".ficha.v1.Company.estabelecimentos: array expected");
                    message.estabelecimentos = Array(object.estabelecimentos.length);
                    for (let i = 0; i < object.estabelecimentos.length; ++i) {
                        if (typeof object.estabelecimentos[i] !== "object")
                            throw TypeError(".ficha.v1.Company.estabelecimentos: object expected");
                        message.estabelecimentos[i] = $root.ficha.v1.Estabelecimento.fromObject(object.estabelecimentos[i], _depth + 1);
                    }
                }
                if (object.socios) {
                    if (!Array.isArray(object.socios))
                        throw TypeError(".ficha.v1.Company.socios: array expected");
                    message.socios = Array(object.socios.length);
                    for (let i = 0; i < object.socios.length; ++i) {
                        if (typeof object.socios[i] !== "object")
                            throw TypeError(".ficha.v1.Company.socios: object expected");
                        message.socios[i] = $root.ficha.v1.Socio.fromObject(object.socios[i], _depth + 1);
                    }
                }
                if (object.snapshot_yyyymm != null)
                    if (Number(object.snapshot_yyyymm) !== 0)
                        message.snapshot_yyyymm = object.snapshot_yyyymm >>> 0;
                return message;
            };

            /**
             * Creates a plain object from a Company message. Also converts values to other types if specified.
             * @function toObject
             * @memberof ficha.v1.Company
             * @static
             * @param {ficha.v1.Company} message Company
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            Company.toObject = function toObject(message, options) {
                if (!options)
                    options = {};
                let object = {};
                if (options.arrays || options.defaults) {
                    object.estabelecimentos = [];
                    object.socios = [];
                }
                if (options.defaults) {
                    object.cnpj_base = 0;
                    object.razao_social = "";
                    object.razao_social_normalizada = "";
                    object.natureza_juridica_codigo = 0;
                    object.porte_empresa = options.enums === String ? "PORTE_UNSPECIFIED" : 0;
                    object.capital_social = 0;
                    object.ente_federativo_responsavel = "";
                    object.qtd_estabelecimentos = 0;
                    object.qtd_estabelecimentos_ativos = 0;
                    object.snapshot_yyyymm = 0;
                }
                if (message.cnpj_base != null && message.hasOwnProperty("cnpj_base"))
                    object.cnpj_base = message.cnpj_base;
                if (message.razao_social != null && message.hasOwnProperty("razao_social"))
                    object.razao_social = message.razao_social;
                if (message.razao_social_normalizada != null && message.hasOwnProperty("razao_social_normalizada"))
                    object.razao_social_normalizada = message.razao_social_normalizada;
                if (message.natureza_juridica_codigo != null && message.hasOwnProperty("natureza_juridica_codigo"))
                    object.natureza_juridica_codigo = message.natureza_juridica_codigo;
                if (message.porte_empresa != null && message.hasOwnProperty("porte_empresa"))
                    object.porte_empresa = options.enums === String ? $root.ficha.v1.Porte[message.porte_empresa] === undefined ? message.porte_empresa : $root.ficha.v1.Porte[message.porte_empresa] : message.porte_empresa;
                if (message.capital_social != null && message.hasOwnProperty("capital_social"))
                    object.capital_social = options.json && !isFinite(message.capital_social) ? String(message.capital_social) : message.capital_social;
                if (message.ente_federativo_responsavel != null && message.hasOwnProperty("ente_federativo_responsavel"))
                    object.ente_federativo_responsavel = message.ente_federativo_responsavel;
                if (message.qtd_estabelecimentos != null && message.hasOwnProperty("qtd_estabelecimentos"))
                    object.qtd_estabelecimentos = message.qtd_estabelecimentos;
                if (message.qtd_estabelecimentos_ativos != null && message.hasOwnProperty("qtd_estabelecimentos_ativos"))
                    object.qtd_estabelecimentos_ativos = message.qtd_estabelecimentos_ativos;
                if (message.estabelecimentos && message.estabelecimentos.length) {
                    object.estabelecimentos = Array(message.estabelecimentos.length);
                    for (let j = 0; j < message.estabelecimentos.length; ++j)
                        object.estabelecimentos[j] = $root.ficha.v1.Estabelecimento.toObject(message.estabelecimentos[j], options);
                }
                if (message.socios && message.socios.length) {
                    object.socios = Array(message.socios.length);
                    for (let j = 0; j < message.socios.length; ++j)
                        object.socios[j] = $root.ficha.v1.Socio.toObject(message.socios[j], options);
                }
                if (message.snapshot_yyyymm != null && message.hasOwnProperty("snapshot_yyyymm"))
                    object.snapshot_yyyymm = message.snapshot_yyyymm;
                return object;
            };

            /**
             * Converts this Company to JSON.
             * @function toJSON
             * @memberof ficha.v1.Company
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            Company.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            /**
             * Gets the type url for Company
             * @function getTypeUrl
             * @memberof ficha.v1.Company
             * @static
             * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns {string} The type url
             */
            Company.getTypeUrl = function getTypeUrl(prefix) {
                if (prefix === undefined)
                    prefix = "type.googleapis.com";
                return prefix + "/ficha.v1.Company";
            };

            return Company;
        })();

        v1.Estabelecimento = (function() {

            /**
             * Properties of an Estabelecimento.
             * @typedef {Object} ficha.v1.Estabelecimento.$Properties
             * @property {number|null} [cnpj_ordem] Estabelecimento cnpj_ordem
             * @property {number|null} [cnpj_dv] Estabelecimento cnpj_dv
             * @property {ficha.v1.TipoEstabelecimento|null} [tipo] Estabelecimento tipo
             * @property {string|null} [nome_fantasia] Estabelecimento nome_fantasia
             * @property {number|null} [situacao_cadastral] Estabelecimento situacao_cadastral
             * @property {number|null} [data_situacao_cadastral] Estabelecimento data_situacao_cadastral
             * @property {number|null} [motivo_situacao_cadastral_codigo] Estabelecimento motivo_situacao_cadastral_codigo
             * @property {string|null} [situacao_especial] Estabelecimento situacao_especial
             * @property {number|null} [data_situacao_especial] Estabelecimento data_situacao_especial
             * @property {number|null} [data_inicio_atividade] Estabelecimento data_inicio_atividade
             * @property {number|null} [cnae_principal_codigo] Estabelecimento cnae_principal_codigo
             * @property {Array.<number>|null} [cnaes_secundarios_codigos] Estabelecimento cnaes_secundarios_codigos
             * @property {string|null} [tipo_logradouro] Estabelecimento tipo_logradouro
             * @property {string|null} [logradouro] Estabelecimento logradouro
             * @property {string|null} [numero] Estabelecimento numero
             * @property {string|null} [complemento] Estabelecimento complemento
             * @property {string|null} [bairro] Estabelecimento bairro
             * @property {number|null} [cep] Estabelecimento cep
             * @property {string|null} [uf] Estabelecimento uf
             * @property {number|null} [municipio_codigo] Estabelecimento municipio_codigo
             * @property {string|null} [nome_cidade_exterior] Estabelecimento nome_cidade_exterior
             * @property {number|null} [pais_codigo] Estabelecimento pais_codigo
             * @property {string|null} [ddd_1] Estabelecimento ddd_1
             * @property {string|null} [telefone_1] Estabelecimento telefone_1
             * @property {string|null} [ddd_2] Estabelecimento ddd_2
             * @property {string|null} [telefone_2] Estabelecimento telefone_2
             * @property {string|null} [ddd_fax] Estabelecimento ddd_fax
             * @property {string|null} [fax] Estabelecimento fax
             * @property {string|null} [correio_eletronico] Estabelecimento correio_eletronico
             * @property {boolean|null} [opcao_simples] Estabelecimento opcao_simples
             * @property {number|null} [data_opcao_simples] Estabelecimento data_opcao_simples
             * @property {number|null} [data_exclusao_simples] Estabelecimento data_exclusao_simples
             * @property {boolean|null} [opcao_mei] Estabelecimento opcao_mei
             * @property {number|null} [data_opcao_mei] Estabelecimento data_opcao_mei
             * @property {number|null} [data_exclusao_mei] Estabelecimento data_exclusao_mei
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */

            /**
             * Properties of an Estabelecimento.
             * @memberof ficha.v1
             * @interface IEstabelecimento
             * @augments ficha.v1.Estabelecimento.$Properties
             * @deprecated Use ficha.v1.Estabelecimento.$Properties instead.
             */

            /**
             * Shape of an Estabelecimento.
             * @typedef {ficha.v1.Estabelecimento.$Properties} ficha.v1.Estabelecimento.$Shape
             */

            /**
             * Constructs a new Estabelecimento.
             * @memberof ficha.v1
             * @classdesc Represents an Estabelecimento.
             * @constructor
             * @param {ficha.v1.Estabelecimento.$Properties=} [properties] Properties to set
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */
            function Estabelecimento(properties) {
                this.cnaes_secundarios_codigos = [];
                if (properties)
                    for (let keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null && keys[i] !== "__proto__")
                            this[keys[i]] = properties[keys[i]];
            }

            /**
             * Estabelecimento cnpj_ordem.
             * @member {number} cnpj_ordem
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.cnpj_ordem = 0;

            /**
             * Estabelecimento cnpj_dv.
             * @member {number} cnpj_dv
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.cnpj_dv = 0;

            /**
             * Estabelecimento tipo.
             * @member {ficha.v1.TipoEstabelecimento} tipo
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.tipo = 0;

            /**
             * Estabelecimento nome_fantasia.
             * @member {string} nome_fantasia
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.nome_fantasia = "";

            /**
             * Estabelecimento situacao_cadastral.
             * @member {number} situacao_cadastral
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.situacao_cadastral = 0;

            /**
             * Estabelecimento data_situacao_cadastral.
             * @member {number} data_situacao_cadastral
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.data_situacao_cadastral = 0;

            /**
             * Estabelecimento motivo_situacao_cadastral_codigo.
             * @member {number} motivo_situacao_cadastral_codigo
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.motivo_situacao_cadastral_codigo = 0;

            /**
             * Estabelecimento situacao_especial.
             * @member {string} situacao_especial
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.situacao_especial = "";

            /**
             * Estabelecimento data_situacao_especial.
             * @member {number} data_situacao_especial
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.data_situacao_especial = 0;

            /**
             * Estabelecimento data_inicio_atividade.
             * @member {number} data_inicio_atividade
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.data_inicio_atividade = 0;

            /**
             * Estabelecimento cnae_principal_codigo.
             * @member {number} cnae_principal_codigo
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.cnae_principal_codigo = 0;

            /**
             * Estabelecimento cnaes_secundarios_codigos.
             * @member {Array.<number>} cnaes_secundarios_codigos
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.cnaes_secundarios_codigos = $util.emptyArray;

            /**
             * Estabelecimento tipo_logradouro.
             * @member {string} tipo_logradouro
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.tipo_logradouro = "";

            /**
             * Estabelecimento logradouro.
             * @member {string} logradouro
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.logradouro = "";

            /**
             * Estabelecimento numero.
             * @member {string} numero
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.numero = "";

            /**
             * Estabelecimento complemento.
             * @member {string} complemento
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.complemento = "";

            /**
             * Estabelecimento bairro.
             * @member {string} bairro
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.bairro = "";

            /**
             * Estabelecimento cep.
             * @member {number} cep
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.cep = 0;

            /**
             * Estabelecimento uf.
             * @member {string} uf
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.uf = "";

            /**
             * Estabelecimento municipio_codigo.
             * @member {number} municipio_codigo
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.municipio_codigo = 0;

            /**
             * Estabelecimento nome_cidade_exterior.
             * @member {string} nome_cidade_exterior
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.nome_cidade_exterior = "";

            /**
             * Estabelecimento pais_codigo.
             * @member {number} pais_codigo
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.pais_codigo = 0;

            /**
             * Estabelecimento ddd_1.
             * @member {string} ddd_1
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.ddd_1 = "";

            /**
             * Estabelecimento telefone_1.
             * @member {string} telefone_1
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.telefone_1 = "";

            /**
             * Estabelecimento ddd_2.
             * @member {string} ddd_2
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.ddd_2 = "";

            /**
             * Estabelecimento telefone_2.
             * @member {string} telefone_2
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.telefone_2 = "";

            /**
             * Estabelecimento ddd_fax.
             * @member {string} ddd_fax
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.ddd_fax = "";

            /**
             * Estabelecimento fax.
             * @member {string} fax
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.fax = "";

            /**
             * Estabelecimento correio_eletronico.
             * @member {string} correio_eletronico
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.correio_eletronico = "";

            /**
             * Estabelecimento opcao_simples.
             * @member {boolean} opcao_simples
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.opcao_simples = false;

            /**
             * Estabelecimento data_opcao_simples.
             * @member {number} data_opcao_simples
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.data_opcao_simples = 0;

            /**
             * Estabelecimento data_exclusao_simples.
             * @member {number} data_exclusao_simples
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.data_exclusao_simples = 0;

            /**
             * Estabelecimento opcao_mei.
             * @member {boolean} opcao_mei
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.opcao_mei = false;

            /**
             * Estabelecimento data_opcao_mei.
             * @member {number} data_opcao_mei
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.data_opcao_mei = 0;

            /**
             * Estabelecimento data_exclusao_mei.
             * @member {number} data_exclusao_mei
             * @memberof ficha.v1.Estabelecimento
             * @instance
             */
            Estabelecimento.prototype.data_exclusao_mei = 0;

            /**
             * Creates a new Estabelecimento instance using the specified properties.
             * @function create
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {ficha.v1.Estabelecimento.$Properties=} [properties] Properties to set
             * @returns {ficha.v1.Estabelecimento} Estabelecimento instance
             * @type {{
             *   (properties: ficha.v1.Estabelecimento.$Shape): ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape;
             *   (properties?: ficha.v1.Estabelecimento.$Properties): ficha.v1.Estabelecimento;
             * }}
             */
            Estabelecimento.create = function create(properties) {
                return new Estabelecimento(properties);
            };

            /**
             * Encodes the specified Estabelecimento message. Does not implicitly {@link ficha.v1.Estabelecimento.verify|verify} messages.
             * @function encode
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {ficha.v1.Estabelecimento.$Properties} message Estabelecimento message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Estabelecimento.encode = function encode(message, writer) {
                if (!writer)
                    writer = $Writer.create();
                if (message.cnpj_ordem != null && Object.hasOwnProperty.call(message, "cnpj_ordem"))
                    writer.uint32(/* id 1, wireType 0 =*/8).uint32(message.cnpj_ordem);
                if (message.cnpj_dv != null && Object.hasOwnProperty.call(message, "cnpj_dv"))
                    writer.uint32(/* id 2, wireType 0 =*/16).uint32(message.cnpj_dv);
                if (message.tipo != null && Object.hasOwnProperty.call(message, "tipo"))
                    writer.uint32(/* id 3, wireType 0 =*/24).int32(message.tipo);
                if (message.nome_fantasia != null && Object.hasOwnProperty.call(message, "nome_fantasia"))
                    writer.uint32(/* id 4, wireType 2 =*/34).string(message.nome_fantasia);
                if (message.situacao_cadastral != null && Object.hasOwnProperty.call(message, "situacao_cadastral"))
                    writer.uint32(/* id 10, wireType 0 =*/80).uint32(message.situacao_cadastral);
                if (message.data_situacao_cadastral != null && Object.hasOwnProperty.call(message, "data_situacao_cadastral"))
                    writer.uint32(/* id 11, wireType 0 =*/88).uint32(message.data_situacao_cadastral);
                if (message.motivo_situacao_cadastral_codigo != null && Object.hasOwnProperty.call(message, "motivo_situacao_cadastral_codigo"))
                    writer.uint32(/* id 12, wireType 0 =*/96).uint32(message.motivo_situacao_cadastral_codigo);
                if (message.situacao_especial != null && Object.hasOwnProperty.call(message, "situacao_especial"))
                    writer.uint32(/* id 13, wireType 2 =*/106).string(message.situacao_especial);
                if (message.data_situacao_especial != null && Object.hasOwnProperty.call(message, "data_situacao_especial"))
                    writer.uint32(/* id 14, wireType 0 =*/112).uint32(message.data_situacao_especial);
                if (message.data_inicio_atividade != null && Object.hasOwnProperty.call(message, "data_inicio_atividade"))
                    writer.uint32(/* id 15, wireType 0 =*/120).uint32(message.data_inicio_atividade);
                if (message.cnae_principal_codigo != null && Object.hasOwnProperty.call(message, "cnae_principal_codigo"))
                    writer.uint32(/* id 20, wireType 0 =*/160).uint32(message.cnae_principal_codigo);
                if (message.cnaes_secundarios_codigos != null && message.cnaes_secundarios_codigos.length) {
                    writer.uint32(/* id 21, wireType 2 =*/170).fork();
                    for (let i = 0; i < message.cnaes_secundarios_codigos.length; ++i)
                        writer.uint32(message.cnaes_secundarios_codigos[i]);
                    writer.ldelim();
                }
                if (message.tipo_logradouro != null && Object.hasOwnProperty.call(message, "tipo_logradouro"))
                    writer.uint32(/* id 30, wireType 2 =*/242).string(message.tipo_logradouro);
                if (message.logradouro != null && Object.hasOwnProperty.call(message, "logradouro"))
                    writer.uint32(/* id 31, wireType 2 =*/250).string(message.logradouro);
                if (message.numero != null && Object.hasOwnProperty.call(message, "numero"))
                    writer.uint32(/* id 32, wireType 2 =*/258).string(message.numero);
                if (message.complemento != null && Object.hasOwnProperty.call(message, "complemento"))
                    writer.uint32(/* id 33, wireType 2 =*/266).string(message.complemento);
                if (message.bairro != null && Object.hasOwnProperty.call(message, "bairro"))
                    writer.uint32(/* id 34, wireType 2 =*/274).string(message.bairro);
                if (message.cep != null && Object.hasOwnProperty.call(message, "cep"))
                    writer.uint32(/* id 35, wireType 0 =*/280).uint32(message.cep);
                if (message.uf != null && Object.hasOwnProperty.call(message, "uf"))
                    writer.uint32(/* id 36, wireType 2 =*/290).string(message.uf);
                if (message.municipio_codigo != null && Object.hasOwnProperty.call(message, "municipio_codigo"))
                    writer.uint32(/* id 37, wireType 0 =*/296).uint32(message.municipio_codigo);
                if (message.nome_cidade_exterior != null && Object.hasOwnProperty.call(message, "nome_cidade_exterior"))
                    writer.uint32(/* id 38, wireType 2 =*/306).string(message.nome_cidade_exterior);
                if (message.pais_codigo != null && Object.hasOwnProperty.call(message, "pais_codigo"))
                    writer.uint32(/* id 39, wireType 0 =*/312).uint32(message.pais_codigo);
                if (message.ddd_1 != null && Object.hasOwnProperty.call(message, "ddd_1"))
                    writer.uint32(/* id 40, wireType 2 =*/322).string(message.ddd_1);
                if (message.telefone_1 != null && Object.hasOwnProperty.call(message, "telefone_1"))
                    writer.uint32(/* id 41, wireType 2 =*/330).string(message.telefone_1);
                if (message.ddd_2 != null && Object.hasOwnProperty.call(message, "ddd_2"))
                    writer.uint32(/* id 42, wireType 2 =*/338).string(message.ddd_2);
                if (message.telefone_2 != null && Object.hasOwnProperty.call(message, "telefone_2"))
                    writer.uint32(/* id 43, wireType 2 =*/346).string(message.telefone_2);
                if (message.ddd_fax != null && Object.hasOwnProperty.call(message, "ddd_fax"))
                    writer.uint32(/* id 44, wireType 2 =*/354).string(message.ddd_fax);
                if (message.fax != null && Object.hasOwnProperty.call(message, "fax"))
                    writer.uint32(/* id 45, wireType 2 =*/362).string(message.fax);
                if (message.correio_eletronico != null && Object.hasOwnProperty.call(message, "correio_eletronico"))
                    writer.uint32(/* id 46, wireType 2 =*/370).string(message.correio_eletronico);
                if (message.opcao_simples != null && Object.hasOwnProperty.call(message, "opcao_simples"))
                    writer.uint32(/* id 50, wireType 0 =*/400).bool(message.opcao_simples);
                if (message.data_opcao_simples != null && Object.hasOwnProperty.call(message, "data_opcao_simples"))
                    writer.uint32(/* id 51, wireType 0 =*/408).uint32(message.data_opcao_simples);
                if (message.data_exclusao_simples != null && Object.hasOwnProperty.call(message, "data_exclusao_simples"))
                    writer.uint32(/* id 52, wireType 0 =*/416).uint32(message.data_exclusao_simples);
                if (message.opcao_mei != null && Object.hasOwnProperty.call(message, "opcao_mei"))
                    writer.uint32(/* id 53, wireType 0 =*/424).bool(message.opcao_mei);
                if (message.data_opcao_mei != null && Object.hasOwnProperty.call(message, "data_opcao_mei"))
                    writer.uint32(/* id 54, wireType 0 =*/432).uint32(message.data_opcao_mei);
                if (message.data_exclusao_mei != null && Object.hasOwnProperty.call(message, "data_exclusao_mei"))
                    writer.uint32(/* id 55, wireType 0 =*/440).uint32(message.data_exclusao_mei);
                if (message.$unknowns != null && Object.hasOwnProperty.call(message, "$unknowns"))
                    for (let i = 0; i < message.$unknowns.length; ++i)
                        writer.raw(message.$unknowns[i]);
                return writer;
            };

            /**
             * Encodes the specified Estabelecimento message, length delimited. Does not implicitly {@link ficha.v1.Estabelecimento.verify|verify} messages.
             * @function encodeDelimited
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {ficha.v1.Estabelecimento.$Properties} message Estabelecimento message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Estabelecimento.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes an Estabelecimento message from the specified reader or buffer.
             * @function decode
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape} Estabelecimento
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Estabelecimento.decode = function decode(reader, length, _end, _depth, _target) {
                if (!(reader instanceof $Reader))
                    reader = $Reader.create(reader);
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $Reader.recursionLimit)
                    throw Error("max depth exceeded");
                let end = length === undefined ? reader.len : reader.pos + length, message = _target || new $root.ficha.v1.Estabelecimento(), value;
                while (reader.pos < end) {
                    let start = reader.pos;
                    let tag = reader.tag();
                    if (tag === _end) {
                        _end = undefined;
                        break;
                    }
                    let wireType = tag & 7;
                    switch (tag >>>= 3) {
                    case 1: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.cnpj_ordem = value;
                            else
                                delete message.cnpj_ordem;
                            continue;
                        }
                    case 2: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.cnpj_dv = value;
                            else
                                delete message.cnpj_dv;
                            continue;
                        }
                    case 3: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.int32())
                                message.tipo = value;
                            else
                                delete message.tipo;
                            continue;
                        }
                    case 4: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.nome_fantasia = value;
                            else
                                delete message.nome_fantasia;
                            continue;
                        }
                    case 10: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.situacao_cadastral = value;
                            else
                                delete message.situacao_cadastral;
                            continue;
                        }
                    case 11: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_situacao_cadastral = value;
                            else
                                delete message.data_situacao_cadastral;
                            continue;
                        }
                    case 12: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.motivo_situacao_cadastral_codigo = value;
                            else
                                delete message.motivo_situacao_cadastral_codigo;
                            continue;
                        }
                    case 13: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.situacao_especial = value;
                            else
                                delete message.situacao_especial;
                            continue;
                        }
                    case 14: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_situacao_especial = value;
                            else
                                delete message.data_situacao_especial;
                            continue;
                        }
                    case 15: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_inicio_atividade = value;
                            else
                                delete message.data_inicio_atividade;
                            continue;
                        }
                    case 20: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.cnae_principal_codigo = value;
                            else
                                delete message.cnae_principal_codigo;
                            continue;
                        }
                    case 21: {
                            if (wireType === 2) {
                                if (!(message.cnaes_secundarios_codigos && message.cnaes_secundarios_codigos.length))
                                    message.cnaes_secundarios_codigos = [];
                                let end2 = reader.uint32() + reader.pos;
                                while (reader.pos < end2)
                                    message.cnaes_secundarios_codigos.push(reader.uint32());
                                continue;
                            }
                            if (wireType !== 0)
                                break;
                            if (!(message.cnaes_secundarios_codigos && message.cnaes_secundarios_codigos.length))
                                message.cnaes_secundarios_codigos = [];
                            message.cnaes_secundarios_codigos.push(reader.uint32());
                            continue;
                        }
                    case 30: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.tipo_logradouro = value;
                            else
                                delete message.tipo_logradouro;
                            continue;
                        }
                    case 31: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.logradouro = value;
                            else
                                delete message.logradouro;
                            continue;
                        }
                    case 32: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.numero = value;
                            else
                                delete message.numero;
                            continue;
                        }
                    case 33: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.complemento = value;
                            else
                                delete message.complemento;
                            continue;
                        }
                    case 34: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.bairro = value;
                            else
                                delete message.bairro;
                            continue;
                        }
                    case 35: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.cep = value;
                            else
                                delete message.cep;
                            continue;
                        }
                    case 36: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.uf = value;
                            else
                                delete message.uf;
                            continue;
                        }
                    case 37: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.municipio_codigo = value;
                            else
                                delete message.municipio_codigo;
                            continue;
                        }
                    case 38: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.nome_cidade_exterior = value;
                            else
                                delete message.nome_cidade_exterior;
                            continue;
                        }
                    case 39: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.pais_codigo = value;
                            else
                                delete message.pais_codigo;
                            continue;
                        }
                    case 40: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.ddd_1 = value;
                            else
                                delete message.ddd_1;
                            continue;
                        }
                    case 41: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.telefone_1 = value;
                            else
                                delete message.telefone_1;
                            continue;
                        }
                    case 42: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.ddd_2 = value;
                            else
                                delete message.ddd_2;
                            continue;
                        }
                    case 43: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.telefone_2 = value;
                            else
                                delete message.telefone_2;
                            continue;
                        }
                    case 44: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.ddd_fax = value;
                            else
                                delete message.ddd_fax;
                            continue;
                        }
                    case 45: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.fax = value;
                            else
                                delete message.fax;
                            continue;
                        }
                    case 46: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.correio_eletronico = value;
                            else
                                delete message.correio_eletronico;
                            continue;
                        }
                    case 50: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.bool())
                                message.opcao_simples = value;
                            else
                                delete message.opcao_simples;
                            continue;
                        }
                    case 51: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_opcao_simples = value;
                            else
                                delete message.data_opcao_simples;
                            continue;
                        }
                    case 52: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_exclusao_simples = value;
                            else
                                delete message.data_exclusao_simples;
                            continue;
                        }
                    case 53: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.bool())
                                message.opcao_mei = value;
                            else
                                delete message.opcao_mei;
                            continue;
                        }
                    case 54: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_opcao_mei = value;
                            else
                                delete message.data_opcao_mei;
                            continue;
                        }
                    case 55: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_exclusao_mei = value;
                            else
                                delete message.data_exclusao_mei;
                            continue;
                        }
                    }
                    reader.skipType(wireType, _depth, tag);
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
                if (_end !== undefined)
                    throw Error("missing end group");
                return message;
            };

            /**
             * Decodes an Estabelecimento message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {ficha.v1.Estabelecimento & ficha.v1.Estabelecimento.$Shape} Estabelecimento
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Estabelecimento.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader))
                    reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies an Estabelecimento message.
             * @function verify
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            Estabelecimento.verify = function verify(message, _depth) {
                if (typeof message !== "object" || message === null)
                    return "object expected";
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    return "max depth exceeded";
                if (message.cnpj_ordem != null && message.hasOwnProperty("cnpj_ordem"))
                    if (!$util.isInteger(message.cnpj_ordem))
                        return "cnpj_ordem: integer expected";
                if (message.cnpj_dv != null && message.hasOwnProperty("cnpj_dv"))
                    if (!$util.isInteger(message.cnpj_dv))
                        return "cnpj_dv: integer expected";
                if (message.tipo != null && message.hasOwnProperty("tipo"))
                    switch (message.tipo) {
                    default:
                        return "tipo: enum value expected";
                    case 0:
                    case 1:
                    case 2:
                        break;
                    }
                if (message.nome_fantasia != null && message.hasOwnProperty("nome_fantasia"))
                    if (!$util.isString(message.nome_fantasia))
                        return "nome_fantasia: string expected";
                if (message.situacao_cadastral != null && message.hasOwnProperty("situacao_cadastral"))
                    if (!$util.isInteger(message.situacao_cadastral))
                        return "situacao_cadastral: integer expected";
                if (message.data_situacao_cadastral != null && message.hasOwnProperty("data_situacao_cadastral"))
                    if (!$util.isInteger(message.data_situacao_cadastral))
                        return "data_situacao_cadastral: integer expected";
                if (message.motivo_situacao_cadastral_codigo != null && message.hasOwnProperty("motivo_situacao_cadastral_codigo"))
                    if (!$util.isInteger(message.motivo_situacao_cadastral_codigo))
                        return "motivo_situacao_cadastral_codigo: integer expected";
                if (message.situacao_especial != null && message.hasOwnProperty("situacao_especial"))
                    if (!$util.isString(message.situacao_especial))
                        return "situacao_especial: string expected";
                if (message.data_situacao_especial != null && message.hasOwnProperty("data_situacao_especial"))
                    if (!$util.isInteger(message.data_situacao_especial))
                        return "data_situacao_especial: integer expected";
                if (message.data_inicio_atividade != null && message.hasOwnProperty("data_inicio_atividade"))
                    if (!$util.isInteger(message.data_inicio_atividade))
                        return "data_inicio_atividade: integer expected";
                if (message.cnae_principal_codigo != null && message.hasOwnProperty("cnae_principal_codigo"))
                    if (!$util.isInteger(message.cnae_principal_codigo))
                        return "cnae_principal_codigo: integer expected";
                if (message.cnaes_secundarios_codigos != null && message.hasOwnProperty("cnaes_secundarios_codigos")) {
                    if (!Array.isArray(message.cnaes_secundarios_codigos))
                        return "cnaes_secundarios_codigos: array expected";
                    for (let i = 0; i < message.cnaes_secundarios_codigos.length; ++i)
                        if (!$util.isInteger(message.cnaes_secundarios_codigos[i]))
                            return "cnaes_secundarios_codigos: integer[] expected";
                }
                if (message.tipo_logradouro != null && message.hasOwnProperty("tipo_logradouro"))
                    if (!$util.isString(message.tipo_logradouro))
                        return "tipo_logradouro: string expected";
                if (message.logradouro != null && message.hasOwnProperty("logradouro"))
                    if (!$util.isString(message.logradouro))
                        return "logradouro: string expected";
                if (message.numero != null && message.hasOwnProperty("numero"))
                    if (!$util.isString(message.numero))
                        return "numero: string expected";
                if (message.complemento != null && message.hasOwnProperty("complemento"))
                    if (!$util.isString(message.complemento))
                        return "complemento: string expected";
                if (message.bairro != null && message.hasOwnProperty("bairro"))
                    if (!$util.isString(message.bairro))
                        return "bairro: string expected";
                if (message.cep != null && message.hasOwnProperty("cep"))
                    if (!$util.isInteger(message.cep))
                        return "cep: integer expected";
                if (message.uf != null && message.hasOwnProperty("uf"))
                    if (!$util.isString(message.uf))
                        return "uf: string expected";
                if (message.municipio_codigo != null && message.hasOwnProperty("municipio_codigo"))
                    if (!$util.isInteger(message.municipio_codigo))
                        return "municipio_codigo: integer expected";
                if (message.nome_cidade_exterior != null && message.hasOwnProperty("nome_cidade_exterior"))
                    if (!$util.isString(message.nome_cidade_exterior))
                        return "nome_cidade_exterior: string expected";
                if (message.pais_codigo != null && message.hasOwnProperty("pais_codigo"))
                    if (!$util.isInteger(message.pais_codigo))
                        return "pais_codigo: integer expected";
                if (message.ddd_1 != null && message.hasOwnProperty("ddd_1"))
                    if (!$util.isString(message.ddd_1))
                        return "ddd_1: string expected";
                if (message.telefone_1 != null && message.hasOwnProperty("telefone_1"))
                    if (!$util.isString(message.telefone_1))
                        return "telefone_1: string expected";
                if (message.ddd_2 != null && message.hasOwnProperty("ddd_2"))
                    if (!$util.isString(message.ddd_2))
                        return "ddd_2: string expected";
                if (message.telefone_2 != null && message.hasOwnProperty("telefone_2"))
                    if (!$util.isString(message.telefone_2))
                        return "telefone_2: string expected";
                if (message.ddd_fax != null && message.hasOwnProperty("ddd_fax"))
                    if (!$util.isString(message.ddd_fax))
                        return "ddd_fax: string expected";
                if (message.fax != null && message.hasOwnProperty("fax"))
                    if (!$util.isString(message.fax))
                        return "fax: string expected";
                if (message.correio_eletronico != null && message.hasOwnProperty("correio_eletronico"))
                    if (!$util.isString(message.correio_eletronico))
                        return "correio_eletronico: string expected";
                if (message.opcao_simples != null && message.hasOwnProperty("opcao_simples"))
                    if (typeof message.opcao_simples !== "boolean")
                        return "opcao_simples: boolean expected";
                if (message.data_opcao_simples != null && message.hasOwnProperty("data_opcao_simples"))
                    if (!$util.isInteger(message.data_opcao_simples))
                        return "data_opcao_simples: integer expected";
                if (message.data_exclusao_simples != null && message.hasOwnProperty("data_exclusao_simples"))
                    if (!$util.isInteger(message.data_exclusao_simples))
                        return "data_exclusao_simples: integer expected";
                if (message.opcao_mei != null && message.hasOwnProperty("opcao_mei"))
                    if (typeof message.opcao_mei !== "boolean")
                        return "opcao_mei: boolean expected";
                if (message.data_opcao_mei != null && message.hasOwnProperty("data_opcao_mei"))
                    if (!$util.isInteger(message.data_opcao_mei))
                        return "data_opcao_mei: integer expected";
                if (message.data_exclusao_mei != null && message.hasOwnProperty("data_exclusao_mei"))
                    if (!$util.isInteger(message.data_exclusao_mei))
                        return "data_exclusao_mei: integer expected";
                return null;
            };

            /**
             * Creates an Estabelecimento message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {ficha.v1.Estabelecimento} Estabelecimento
             */
            Estabelecimento.fromObject = function fromObject(object, _depth) {
                if (object instanceof $root.ficha.v1.Estabelecimento)
                    return object;
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    throw Error("max depth exceeded");
                let message = new $root.ficha.v1.Estabelecimento();
                if (object.cnpj_ordem != null)
                    if (Number(object.cnpj_ordem) !== 0)
                        message.cnpj_ordem = object.cnpj_ordem >>> 0;
                if (object.cnpj_dv != null)
                    if (Number(object.cnpj_dv) !== 0)
                        message.cnpj_dv = object.cnpj_dv >>> 0;
                if (object.tipo !== 0 && (typeof object.tipo !== "string" || $root.ficha.v1.TipoEstabelecimento[object.tipo] !== 0))
                    switch (object.tipo) {
                    default:
                        if (typeof object.tipo === "number") {
                            message.tipo = object.tipo;
                            break;
                        }
                        break;
                    case "TIPO_ESTAB_UNSPECIFIED":
                    case 0:
                        message.tipo = 0;
                        break;
                    case "MATRIZ":
                    case 1:
                        message.tipo = 1;
                        break;
                    case "FILIAL":
                    case 2:
                        message.tipo = 2;
                        break;
                    }
                if (object.nome_fantasia != null)
                    if (typeof object.nome_fantasia !== "string" || object.nome_fantasia.length)
                        message.nome_fantasia = String(object.nome_fantasia);
                if (object.situacao_cadastral != null)
                    if (Number(object.situacao_cadastral) !== 0)
                        message.situacao_cadastral = object.situacao_cadastral >>> 0;
                if (object.data_situacao_cadastral != null)
                    if (Number(object.data_situacao_cadastral) !== 0)
                        message.data_situacao_cadastral = object.data_situacao_cadastral >>> 0;
                if (object.motivo_situacao_cadastral_codigo != null)
                    if (Number(object.motivo_situacao_cadastral_codigo) !== 0)
                        message.motivo_situacao_cadastral_codigo = object.motivo_situacao_cadastral_codigo >>> 0;
                if (object.situacao_especial != null)
                    if (typeof object.situacao_especial !== "string" || object.situacao_especial.length)
                        message.situacao_especial = String(object.situacao_especial);
                if (object.data_situacao_especial != null)
                    if (Number(object.data_situacao_especial) !== 0)
                        message.data_situacao_especial = object.data_situacao_especial >>> 0;
                if (object.data_inicio_atividade != null)
                    if (Number(object.data_inicio_atividade) !== 0)
                        message.data_inicio_atividade = object.data_inicio_atividade >>> 0;
                if (object.cnae_principal_codigo != null)
                    if (Number(object.cnae_principal_codigo) !== 0)
                        message.cnae_principal_codigo = object.cnae_principal_codigo >>> 0;
                if (object.cnaes_secundarios_codigos) {
                    if (!Array.isArray(object.cnaes_secundarios_codigos))
                        throw TypeError(".ficha.v1.Estabelecimento.cnaes_secundarios_codigos: array expected");
                    message.cnaes_secundarios_codigos = Array(object.cnaes_secundarios_codigos.length);
                    for (let i = 0; i < object.cnaes_secundarios_codigos.length; ++i)
                        message.cnaes_secundarios_codigos[i] = object.cnaes_secundarios_codigos[i] >>> 0;
                }
                if (object.tipo_logradouro != null)
                    if (typeof object.tipo_logradouro !== "string" || object.tipo_logradouro.length)
                        message.tipo_logradouro = String(object.tipo_logradouro);
                if (object.logradouro != null)
                    if (typeof object.logradouro !== "string" || object.logradouro.length)
                        message.logradouro = String(object.logradouro);
                if (object.numero != null)
                    if (typeof object.numero !== "string" || object.numero.length)
                        message.numero = String(object.numero);
                if (object.complemento != null)
                    if (typeof object.complemento !== "string" || object.complemento.length)
                        message.complemento = String(object.complemento);
                if (object.bairro != null)
                    if (typeof object.bairro !== "string" || object.bairro.length)
                        message.bairro = String(object.bairro);
                if (object.cep != null)
                    if (Number(object.cep) !== 0)
                        message.cep = object.cep >>> 0;
                if (object.uf != null)
                    if (typeof object.uf !== "string" || object.uf.length)
                        message.uf = String(object.uf);
                if (object.municipio_codigo != null)
                    if (Number(object.municipio_codigo) !== 0)
                        message.municipio_codigo = object.municipio_codigo >>> 0;
                if (object.nome_cidade_exterior != null)
                    if (typeof object.nome_cidade_exterior !== "string" || object.nome_cidade_exterior.length)
                        message.nome_cidade_exterior = String(object.nome_cidade_exterior);
                if (object.pais_codigo != null)
                    if (Number(object.pais_codigo) !== 0)
                        message.pais_codigo = object.pais_codigo >>> 0;
                if (object.ddd_1 != null)
                    if (typeof object.ddd_1 !== "string" || object.ddd_1.length)
                        message.ddd_1 = String(object.ddd_1);
                if (object.telefone_1 != null)
                    if (typeof object.telefone_1 !== "string" || object.telefone_1.length)
                        message.telefone_1 = String(object.telefone_1);
                if (object.ddd_2 != null)
                    if (typeof object.ddd_2 !== "string" || object.ddd_2.length)
                        message.ddd_2 = String(object.ddd_2);
                if (object.telefone_2 != null)
                    if (typeof object.telefone_2 !== "string" || object.telefone_2.length)
                        message.telefone_2 = String(object.telefone_2);
                if (object.ddd_fax != null)
                    if (typeof object.ddd_fax !== "string" || object.ddd_fax.length)
                        message.ddd_fax = String(object.ddd_fax);
                if (object.fax != null)
                    if (typeof object.fax !== "string" || object.fax.length)
                        message.fax = String(object.fax);
                if (object.correio_eletronico != null)
                    if (typeof object.correio_eletronico !== "string" || object.correio_eletronico.length)
                        message.correio_eletronico = String(object.correio_eletronico);
                if (object.opcao_simples != null)
                    if (object.opcao_simples)
                        message.opcao_simples = Boolean(object.opcao_simples);
                if (object.data_opcao_simples != null)
                    if (Number(object.data_opcao_simples) !== 0)
                        message.data_opcao_simples = object.data_opcao_simples >>> 0;
                if (object.data_exclusao_simples != null)
                    if (Number(object.data_exclusao_simples) !== 0)
                        message.data_exclusao_simples = object.data_exclusao_simples >>> 0;
                if (object.opcao_mei != null)
                    if (object.opcao_mei)
                        message.opcao_mei = Boolean(object.opcao_mei);
                if (object.data_opcao_mei != null)
                    if (Number(object.data_opcao_mei) !== 0)
                        message.data_opcao_mei = object.data_opcao_mei >>> 0;
                if (object.data_exclusao_mei != null)
                    if (Number(object.data_exclusao_mei) !== 0)
                        message.data_exclusao_mei = object.data_exclusao_mei >>> 0;
                return message;
            };

            /**
             * Creates a plain object from an Estabelecimento message. Also converts values to other types if specified.
             * @function toObject
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {ficha.v1.Estabelecimento} message Estabelecimento
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            Estabelecimento.toObject = function toObject(message, options) {
                if (!options)
                    options = {};
                let object = {};
                if (options.arrays || options.defaults)
                    object.cnaes_secundarios_codigos = [];
                if (options.defaults) {
                    object.cnpj_ordem = 0;
                    object.cnpj_dv = 0;
                    object.tipo = options.enums === String ? "TIPO_ESTAB_UNSPECIFIED" : 0;
                    object.nome_fantasia = "";
                    object.situacao_cadastral = 0;
                    object.data_situacao_cadastral = 0;
                    object.motivo_situacao_cadastral_codigo = 0;
                    object.situacao_especial = "";
                    object.data_situacao_especial = 0;
                    object.data_inicio_atividade = 0;
                    object.cnae_principal_codigo = 0;
                    object.tipo_logradouro = "";
                    object.logradouro = "";
                    object.numero = "";
                    object.complemento = "";
                    object.bairro = "";
                    object.cep = 0;
                    object.uf = "";
                    object.municipio_codigo = 0;
                    object.nome_cidade_exterior = "";
                    object.pais_codigo = 0;
                    object.ddd_1 = "";
                    object.telefone_1 = "";
                    object.ddd_2 = "";
                    object.telefone_2 = "";
                    object.ddd_fax = "";
                    object.fax = "";
                    object.correio_eletronico = "";
                    object.opcao_simples = false;
                    object.data_opcao_simples = 0;
                    object.data_exclusao_simples = 0;
                    object.opcao_mei = false;
                    object.data_opcao_mei = 0;
                    object.data_exclusao_mei = 0;
                }
                if (message.cnpj_ordem != null && message.hasOwnProperty("cnpj_ordem"))
                    object.cnpj_ordem = message.cnpj_ordem;
                if (message.cnpj_dv != null && message.hasOwnProperty("cnpj_dv"))
                    object.cnpj_dv = message.cnpj_dv;
                if (message.tipo != null && message.hasOwnProperty("tipo"))
                    object.tipo = options.enums === String ? $root.ficha.v1.TipoEstabelecimento[message.tipo] === undefined ? message.tipo : $root.ficha.v1.TipoEstabelecimento[message.tipo] : message.tipo;
                if (message.nome_fantasia != null && message.hasOwnProperty("nome_fantasia"))
                    object.nome_fantasia = message.nome_fantasia;
                if (message.situacao_cadastral != null && message.hasOwnProperty("situacao_cadastral"))
                    object.situacao_cadastral = message.situacao_cadastral;
                if (message.data_situacao_cadastral != null && message.hasOwnProperty("data_situacao_cadastral"))
                    object.data_situacao_cadastral = message.data_situacao_cadastral;
                if (message.motivo_situacao_cadastral_codigo != null && message.hasOwnProperty("motivo_situacao_cadastral_codigo"))
                    object.motivo_situacao_cadastral_codigo = message.motivo_situacao_cadastral_codigo;
                if (message.situacao_especial != null && message.hasOwnProperty("situacao_especial"))
                    object.situacao_especial = message.situacao_especial;
                if (message.data_situacao_especial != null && message.hasOwnProperty("data_situacao_especial"))
                    object.data_situacao_especial = message.data_situacao_especial;
                if (message.data_inicio_atividade != null && message.hasOwnProperty("data_inicio_atividade"))
                    object.data_inicio_atividade = message.data_inicio_atividade;
                if (message.cnae_principal_codigo != null && message.hasOwnProperty("cnae_principal_codigo"))
                    object.cnae_principal_codigo = message.cnae_principal_codigo;
                if (message.cnaes_secundarios_codigos && message.cnaes_secundarios_codigos.length) {
                    object.cnaes_secundarios_codigos = Array(message.cnaes_secundarios_codigos.length);
                    for (let j = 0; j < message.cnaes_secundarios_codigos.length; ++j)
                        object.cnaes_secundarios_codigos[j] = message.cnaes_secundarios_codigos[j];
                }
                if (message.tipo_logradouro != null && message.hasOwnProperty("tipo_logradouro"))
                    object.tipo_logradouro = message.tipo_logradouro;
                if (message.logradouro != null && message.hasOwnProperty("logradouro"))
                    object.logradouro = message.logradouro;
                if (message.numero != null && message.hasOwnProperty("numero"))
                    object.numero = message.numero;
                if (message.complemento != null && message.hasOwnProperty("complemento"))
                    object.complemento = message.complemento;
                if (message.bairro != null && message.hasOwnProperty("bairro"))
                    object.bairro = message.bairro;
                if (message.cep != null && message.hasOwnProperty("cep"))
                    object.cep = message.cep;
                if (message.uf != null && message.hasOwnProperty("uf"))
                    object.uf = message.uf;
                if (message.municipio_codigo != null && message.hasOwnProperty("municipio_codigo"))
                    object.municipio_codigo = message.municipio_codigo;
                if (message.nome_cidade_exterior != null && message.hasOwnProperty("nome_cidade_exterior"))
                    object.nome_cidade_exterior = message.nome_cidade_exterior;
                if (message.pais_codigo != null && message.hasOwnProperty("pais_codigo"))
                    object.pais_codigo = message.pais_codigo;
                if (message.ddd_1 != null && message.hasOwnProperty("ddd_1"))
                    object.ddd_1 = message.ddd_1;
                if (message.telefone_1 != null && message.hasOwnProperty("telefone_1"))
                    object.telefone_1 = message.telefone_1;
                if (message.ddd_2 != null && message.hasOwnProperty("ddd_2"))
                    object.ddd_2 = message.ddd_2;
                if (message.telefone_2 != null && message.hasOwnProperty("telefone_2"))
                    object.telefone_2 = message.telefone_2;
                if (message.ddd_fax != null && message.hasOwnProperty("ddd_fax"))
                    object.ddd_fax = message.ddd_fax;
                if (message.fax != null && message.hasOwnProperty("fax"))
                    object.fax = message.fax;
                if (message.correio_eletronico != null && message.hasOwnProperty("correio_eletronico"))
                    object.correio_eletronico = message.correio_eletronico;
                if (message.opcao_simples != null && message.hasOwnProperty("opcao_simples"))
                    object.opcao_simples = message.opcao_simples;
                if (message.data_opcao_simples != null && message.hasOwnProperty("data_opcao_simples"))
                    object.data_opcao_simples = message.data_opcao_simples;
                if (message.data_exclusao_simples != null && message.hasOwnProperty("data_exclusao_simples"))
                    object.data_exclusao_simples = message.data_exclusao_simples;
                if (message.opcao_mei != null && message.hasOwnProperty("opcao_mei"))
                    object.opcao_mei = message.opcao_mei;
                if (message.data_opcao_mei != null && message.hasOwnProperty("data_opcao_mei"))
                    object.data_opcao_mei = message.data_opcao_mei;
                if (message.data_exclusao_mei != null && message.hasOwnProperty("data_exclusao_mei"))
                    object.data_exclusao_mei = message.data_exclusao_mei;
                return object;
            };

            /**
             * Converts this Estabelecimento to JSON.
             * @function toJSON
             * @memberof ficha.v1.Estabelecimento
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            Estabelecimento.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            /**
             * Gets the type url for Estabelecimento
             * @function getTypeUrl
             * @memberof ficha.v1.Estabelecimento
             * @static
             * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns {string} The type url
             */
            Estabelecimento.getTypeUrl = function getTypeUrl(prefix) {
                if (prefix === undefined)
                    prefix = "type.googleapis.com";
                return prefix + "/ficha.v1.Estabelecimento";
            };

            return Estabelecimento;
        })();

        v1.Socio = (function() {

            /**
             * Properties of a Socio.
             * @typedef {Object} ficha.v1.Socio.$Properties
             * @property {ficha.v1.TipoSocio|null} [tipo] Socio tipo
             * @property {string|null} [nome_socio_razao_social] Socio nome_socio_razao_social
             * @property {number|null} [cpf_mascarado_meio] Socio cpf_mascarado_meio
             * @property {number|null} [cnpj_socio] Socio cnpj_socio
             * @property {number|null} [qualificacao_codigo] Socio qualificacao_codigo
             * @property {number|null} [data_entrada_sociedade] Socio data_entrada_sociedade
             * @property {number|null} [pais_codigo] Socio pais_codigo
             * @property {ficha.v1.FaixaEtaria|null} [faixa_etaria] Socio faixa_etaria
             * @property {number|null} [representante_legal_cpf_meio] Socio representante_legal_cpf_meio
             * @property {string|null} [representante_legal_nome] Socio representante_legal_nome
             * @property {number|null} [representante_legal_qualificacao_codigo] Socio representante_legal_qualificacao_codigo
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */

            /**
             * Properties of a Socio.
             * @memberof ficha.v1
             * @interface ISocio
             * @augments ficha.v1.Socio.$Properties
             * @deprecated Use ficha.v1.Socio.$Properties instead.
             */

            /**
             * Shape of a Socio.
             * @typedef {ficha.v1.Socio.$Properties} ficha.v1.Socio.$Shape
             */

            /**
             * Constructs a new Socio.
             * @memberof ficha.v1
             * @classdesc Represents a Socio.
             * @constructor
             * @param {ficha.v1.Socio.$Properties=} [properties] Properties to set
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */
            function Socio(properties) {
                if (properties)
                    for (let keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null && keys[i] !== "__proto__")
                            this[keys[i]] = properties[keys[i]];
            }

            /**
             * Socio tipo.
             * @member {ficha.v1.TipoSocio} tipo
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.tipo = 0;

            /**
             * Socio nome_socio_razao_social.
             * @member {string} nome_socio_razao_social
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.nome_socio_razao_social = "";

            /**
             * Socio cpf_mascarado_meio.
             * @member {number} cpf_mascarado_meio
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.cpf_mascarado_meio = 0;

            /**
             * Socio cnpj_socio.
             * @member {number} cnpj_socio
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.cnpj_socio = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

            /**
             * Socio qualificacao_codigo.
             * @member {number} qualificacao_codigo
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.qualificacao_codigo = 0;

            /**
             * Socio data_entrada_sociedade.
             * @member {number} data_entrada_sociedade
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.data_entrada_sociedade = 0;

            /**
             * Socio pais_codigo.
             * @member {number} pais_codigo
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.pais_codigo = 0;

            /**
             * Socio faixa_etaria.
             * @member {ficha.v1.FaixaEtaria} faixa_etaria
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.faixa_etaria = 0;

            /**
             * Socio representante_legal_cpf_meio.
             * @member {number} representante_legal_cpf_meio
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.representante_legal_cpf_meio = 0;

            /**
             * Socio representante_legal_nome.
             * @member {string} representante_legal_nome
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.representante_legal_nome = "";

            /**
             * Socio representante_legal_qualificacao_codigo.
             * @member {number} representante_legal_qualificacao_codigo
             * @memberof ficha.v1.Socio
             * @instance
             */
            Socio.prototype.representante_legal_qualificacao_codigo = 0;

            /**
             * Creates a new Socio instance using the specified properties.
             * @function create
             * @memberof ficha.v1.Socio
             * @static
             * @param {ficha.v1.Socio.$Properties=} [properties] Properties to set
             * @returns {ficha.v1.Socio} Socio instance
             * @type {{
             *   (properties: ficha.v1.Socio.$Shape): ficha.v1.Socio & ficha.v1.Socio.$Shape;
             *   (properties?: ficha.v1.Socio.$Properties): ficha.v1.Socio;
             * }}
             */
            Socio.create = function create(properties) {
                return new Socio(properties);
            };

            /**
             * Encodes the specified Socio message. Does not implicitly {@link ficha.v1.Socio.verify|verify} messages.
             * @function encode
             * @memberof ficha.v1.Socio
             * @static
             * @param {ficha.v1.Socio.$Properties} message Socio message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Socio.encode = function encode(message, writer) {
                if (!writer)
                    writer = $Writer.create();
                if (message.tipo != null && Object.hasOwnProperty.call(message, "tipo"))
                    writer.uint32(/* id 1, wireType 0 =*/8).int32(message.tipo);
                if (message.nome_socio_razao_social != null && Object.hasOwnProperty.call(message, "nome_socio_razao_social"))
                    writer.uint32(/* id 2, wireType 2 =*/18).string(message.nome_socio_razao_social);
                if (message.cpf_mascarado_meio != null && Object.hasOwnProperty.call(message, "cpf_mascarado_meio"))
                    writer.uint32(/* id 3, wireType 0 =*/24).uint32(message.cpf_mascarado_meio);
                if (message.cnpj_socio != null && Object.hasOwnProperty.call(message, "cnpj_socio"))
                    writer.uint32(/* id 4, wireType 0 =*/32).uint64(message.cnpj_socio);
                if (message.qualificacao_codigo != null && Object.hasOwnProperty.call(message, "qualificacao_codigo"))
                    writer.uint32(/* id 5, wireType 0 =*/40).uint32(message.qualificacao_codigo);
                if (message.data_entrada_sociedade != null && Object.hasOwnProperty.call(message, "data_entrada_sociedade"))
                    writer.uint32(/* id 6, wireType 0 =*/48).uint32(message.data_entrada_sociedade);
                if (message.pais_codigo != null && Object.hasOwnProperty.call(message, "pais_codigo"))
                    writer.uint32(/* id 7, wireType 0 =*/56).uint32(message.pais_codigo);
                if (message.faixa_etaria != null && Object.hasOwnProperty.call(message, "faixa_etaria"))
                    writer.uint32(/* id 8, wireType 0 =*/64).int32(message.faixa_etaria);
                if (message.representante_legal_cpf_meio != null && Object.hasOwnProperty.call(message, "representante_legal_cpf_meio"))
                    writer.uint32(/* id 10, wireType 0 =*/80).uint32(message.representante_legal_cpf_meio);
                if (message.representante_legal_nome != null && Object.hasOwnProperty.call(message, "representante_legal_nome"))
                    writer.uint32(/* id 11, wireType 2 =*/90).string(message.representante_legal_nome);
                if (message.representante_legal_qualificacao_codigo != null && Object.hasOwnProperty.call(message, "representante_legal_qualificacao_codigo"))
                    writer.uint32(/* id 12, wireType 0 =*/96).uint32(message.representante_legal_qualificacao_codigo);
                if (message.$unknowns != null && Object.hasOwnProperty.call(message, "$unknowns"))
                    for (let i = 0; i < message.$unknowns.length; ++i)
                        writer.raw(message.$unknowns[i]);
                return writer;
            };

            /**
             * Encodes the specified Socio message, length delimited. Does not implicitly {@link ficha.v1.Socio.verify|verify} messages.
             * @function encodeDelimited
             * @memberof ficha.v1.Socio
             * @static
             * @param {ficha.v1.Socio.$Properties} message Socio message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Socio.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a Socio message from the specified reader or buffer.
             * @function decode
             * @memberof ficha.v1.Socio
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {ficha.v1.Socio & ficha.v1.Socio.$Shape} Socio
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Socio.decode = function decode(reader, length, _end, _depth, _target) {
                if (!(reader instanceof $Reader))
                    reader = $Reader.create(reader);
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $Reader.recursionLimit)
                    throw Error("max depth exceeded");
                let end = length === undefined ? reader.len : reader.pos + length, message = _target || new $root.ficha.v1.Socio(), value;
                while (reader.pos < end) {
                    let start = reader.pos;
                    let tag = reader.tag();
                    if (tag === _end) {
                        _end = undefined;
                        break;
                    }
                    let wireType = tag & 7;
                    switch (tag >>>= 3) {
                    case 1: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.int32())
                                message.tipo = value;
                            else
                                delete message.tipo;
                            continue;
                        }
                    case 2: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.nome_socio_razao_social = value;
                            else
                                delete message.nome_socio_razao_social;
                            continue;
                        }
                    case 3: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.cpf_mascarado_meio = value;
                            else
                                delete message.cpf_mascarado_meio;
                            continue;
                        }
                    case 4: {
                            if (wireType !== 0)
                                break;
                            if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                                message.cnpj_socio = value;
                            else
                                delete message.cnpj_socio;
                            continue;
                        }
                    case 5: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.qualificacao_codigo = value;
                            else
                                delete message.qualificacao_codigo;
                            continue;
                        }
                    case 6: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.data_entrada_sociedade = value;
                            else
                                delete message.data_entrada_sociedade;
                            continue;
                        }
                    case 7: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.pais_codigo = value;
                            else
                                delete message.pais_codigo;
                            continue;
                        }
                    case 8: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.int32())
                                message.faixa_etaria = value;
                            else
                                delete message.faixa_etaria;
                            continue;
                        }
                    case 10: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.representante_legal_cpf_meio = value;
                            else
                                delete message.representante_legal_cpf_meio;
                            continue;
                        }
                    case 11: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.representante_legal_nome = value;
                            else
                                delete message.representante_legal_nome;
                            continue;
                        }
                    case 12: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.representante_legal_qualificacao_codigo = value;
                            else
                                delete message.representante_legal_qualificacao_codigo;
                            continue;
                        }
                    }
                    reader.skipType(wireType, _depth, tag);
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
                if (_end !== undefined)
                    throw Error("missing end group");
                return message;
            };

            /**
             * Decodes a Socio message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof ficha.v1.Socio
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {ficha.v1.Socio & ficha.v1.Socio.$Shape} Socio
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Socio.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader))
                    reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a Socio message.
             * @function verify
             * @memberof ficha.v1.Socio
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            Socio.verify = function verify(message, _depth) {
                if (typeof message !== "object" || message === null)
                    return "object expected";
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    return "max depth exceeded";
                if (message.tipo != null && message.hasOwnProperty("tipo"))
                    switch (message.tipo) {
                    default:
                        return "tipo: enum value expected";
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                        break;
                    }
                if (message.nome_socio_razao_social != null && message.hasOwnProperty("nome_socio_razao_social"))
                    if (!$util.isString(message.nome_socio_razao_social))
                        return "nome_socio_razao_social: string expected";
                if (message.cpf_mascarado_meio != null && message.hasOwnProperty("cpf_mascarado_meio"))
                    if (!$util.isInteger(message.cpf_mascarado_meio))
                        return "cpf_mascarado_meio: integer expected";
                if (message.cnpj_socio != null && message.hasOwnProperty("cnpj_socio"))
                    if (!$util.isInteger(message.cnpj_socio) && !(message.cnpj_socio && $util.isInteger(message.cnpj_socio.low) && $util.isInteger(message.cnpj_socio.high)))
                        return "cnpj_socio: integer|Long expected";
                if (message.qualificacao_codigo != null && message.hasOwnProperty("qualificacao_codigo"))
                    if (!$util.isInteger(message.qualificacao_codigo))
                        return "qualificacao_codigo: integer expected";
                if (message.data_entrada_sociedade != null && message.hasOwnProperty("data_entrada_sociedade"))
                    if (!$util.isInteger(message.data_entrada_sociedade))
                        return "data_entrada_sociedade: integer expected";
                if (message.pais_codigo != null && message.hasOwnProperty("pais_codigo"))
                    if (!$util.isInteger(message.pais_codigo))
                        return "pais_codigo: integer expected";
                if (message.faixa_etaria != null && message.hasOwnProperty("faixa_etaria"))
                    switch (message.faixa_etaria) {
                    default:
                        return "faixa_etaria: enum value expected";
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                    case 9:
                    case 10:
                        break;
                    }
                if (message.representante_legal_cpf_meio != null && message.hasOwnProperty("representante_legal_cpf_meio"))
                    if (!$util.isInteger(message.representante_legal_cpf_meio))
                        return "representante_legal_cpf_meio: integer expected";
                if (message.representante_legal_nome != null && message.hasOwnProperty("representante_legal_nome"))
                    if (!$util.isString(message.representante_legal_nome))
                        return "representante_legal_nome: string expected";
                if (message.representante_legal_qualificacao_codigo != null && message.hasOwnProperty("representante_legal_qualificacao_codigo"))
                    if (!$util.isInteger(message.representante_legal_qualificacao_codigo))
                        return "representante_legal_qualificacao_codigo: integer expected";
                return null;
            };

            /**
             * Creates a Socio message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof ficha.v1.Socio
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {ficha.v1.Socio} Socio
             */
            Socio.fromObject = function fromObject(object, _depth) {
                if (object instanceof $root.ficha.v1.Socio)
                    return object;
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    throw Error("max depth exceeded");
                let message = new $root.ficha.v1.Socio();
                if (object.tipo !== 0 && (typeof object.tipo !== "string" || $root.ficha.v1.TipoSocio[object.tipo] !== 0))
                    switch (object.tipo) {
                    default:
                        if (typeof object.tipo === "number") {
                            message.tipo = object.tipo;
                            break;
                        }
                        break;
                    case "TIPO_SOCIO_UNSPECIFIED":
                    case 0:
                        message.tipo = 0;
                        break;
                    case "PESSOA_JURIDICA":
                    case 1:
                        message.tipo = 1;
                        break;
                    case "PESSOA_FISICA":
                    case 2:
                        message.tipo = 2;
                        break;
                    case "ESTRANGEIRO":
                    case 3:
                        message.tipo = 3;
                        break;
                    }
                if (object.nome_socio_razao_social != null)
                    if (typeof object.nome_socio_razao_social !== "string" || object.nome_socio_razao_social.length)
                        message.nome_socio_razao_social = String(object.nome_socio_razao_social);
                if (object.cpf_mascarado_meio != null)
                    if (Number(object.cpf_mascarado_meio) !== 0)
                        message.cpf_mascarado_meio = object.cpf_mascarado_meio >>> 0;
                if (object.cnpj_socio != null)
                    if (typeof object.cnpj_socio === "object" ? object.cnpj_socio.low || object.cnpj_socio.high : Number(object.cnpj_socio) !== 0)
                        if ($util.Long)
                            (message.cnpj_socio = $util.Long.fromValue(object.cnpj_socio)).unsigned = true;
                        else if (typeof object.cnpj_socio === "string")
                            message.cnpj_socio = parseInt(object.cnpj_socio, 10);
                        else if (typeof object.cnpj_socio === "number")
                            message.cnpj_socio = object.cnpj_socio;
                        else if (typeof object.cnpj_socio === "object")
                            message.cnpj_socio = new $util.LongBits(object.cnpj_socio.low >>> 0, object.cnpj_socio.high >>> 0).toNumber(true);
                if (object.qualificacao_codigo != null)
                    if (Number(object.qualificacao_codigo) !== 0)
                        message.qualificacao_codigo = object.qualificacao_codigo >>> 0;
                if (object.data_entrada_sociedade != null)
                    if (Number(object.data_entrada_sociedade) !== 0)
                        message.data_entrada_sociedade = object.data_entrada_sociedade >>> 0;
                if (object.pais_codigo != null)
                    if (Number(object.pais_codigo) !== 0)
                        message.pais_codigo = object.pais_codigo >>> 0;
                if (object.faixa_etaria !== 0 && (typeof object.faixa_etaria !== "string" || $root.ficha.v1.FaixaEtaria[object.faixa_etaria] !== 0))
                    switch (object.faixa_etaria) {
                    default:
                        if (typeof object.faixa_etaria === "number") {
                            message.faixa_etaria = object.faixa_etaria;
                            break;
                        }
                        break;
                    case "FAIXA_ETARIA_UNSPECIFIED":
                    case 0:
                        message.faixa_etaria = 0;
                        break;
                    case "ATE_12":
                    case 1:
                        message.faixa_etaria = 1;
                        break;
                    case "DE_13_A_20":
                    case 2:
                        message.faixa_etaria = 2;
                        break;
                    case "DE_21_A_30":
                    case 3:
                        message.faixa_etaria = 3;
                        break;
                    case "DE_31_A_40":
                    case 4:
                        message.faixa_etaria = 4;
                        break;
                    case "DE_41_A_50":
                    case 5:
                        message.faixa_etaria = 5;
                        break;
                    case "DE_51_A_60":
                    case 6:
                        message.faixa_etaria = 6;
                        break;
                    case "DE_61_A_70":
                    case 7:
                        message.faixa_etaria = 7;
                        break;
                    case "DE_71_A_80":
                    case 8:
                        message.faixa_etaria = 8;
                        break;
                    case "ACIMA_80":
                    case 9:
                        message.faixa_etaria = 9;
                        break;
                    case "NAO_INFORMADA":
                    case 10:
                        message.faixa_etaria = 10;
                        break;
                    }
                if (object.representante_legal_cpf_meio != null)
                    if (Number(object.representante_legal_cpf_meio) !== 0)
                        message.representante_legal_cpf_meio = object.representante_legal_cpf_meio >>> 0;
                if (object.representante_legal_nome != null)
                    if (typeof object.representante_legal_nome !== "string" || object.representante_legal_nome.length)
                        message.representante_legal_nome = String(object.representante_legal_nome);
                if (object.representante_legal_qualificacao_codigo != null)
                    if (Number(object.representante_legal_qualificacao_codigo) !== 0)
                        message.representante_legal_qualificacao_codigo = object.representante_legal_qualificacao_codigo >>> 0;
                return message;
            };

            /**
             * Creates a plain object from a Socio message. Also converts values to other types if specified.
             * @function toObject
             * @memberof ficha.v1.Socio
             * @static
             * @param {ficha.v1.Socio} message Socio
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            Socio.toObject = function toObject(message, options) {
                if (!options)
                    options = {};
                let object = {};
                if (options.defaults) {
                    object.tipo = options.enums === String ? "TIPO_SOCIO_UNSPECIFIED" : 0;
                    object.nome_socio_razao_social = "";
                    object.cpf_mascarado_meio = 0;
                    if ($util.Long) {
                        let long = new $util.Long(0, 0, true);
                        object.cnpj_socio = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                    } else
                        object.cnpj_socio = options.longs === String ? "0" : 0;
                    object.qualificacao_codigo = 0;
                    object.data_entrada_sociedade = 0;
                    object.pais_codigo = 0;
                    object.faixa_etaria = options.enums === String ? "FAIXA_ETARIA_UNSPECIFIED" : 0;
                    object.representante_legal_cpf_meio = 0;
                    object.representante_legal_nome = "";
                    object.representante_legal_qualificacao_codigo = 0;
                }
                if (message.tipo != null && message.hasOwnProperty("tipo"))
                    object.tipo = options.enums === String ? $root.ficha.v1.TipoSocio[message.tipo] === undefined ? message.tipo : $root.ficha.v1.TipoSocio[message.tipo] : message.tipo;
                if (message.nome_socio_razao_social != null && message.hasOwnProperty("nome_socio_razao_social"))
                    object.nome_socio_razao_social = message.nome_socio_razao_social;
                if (message.cpf_mascarado_meio != null && message.hasOwnProperty("cpf_mascarado_meio"))
                    object.cpf_mascarado_meio = message.cpf_mascarado_meio;
                if (message.cnpj_socio != null && message.hasOwnProperty("cnpj_socio"))
                    if (typeof message.cnpj_socio === "number")
                        object.cnpj_socio = options.longs === String ? String(message.cnpj_socio) : message.cnpj_socio;
                    else
                        object.cnpj_socio = options.longs === String ? $util.Long.prototype.toString.call(message.cnpj_socio) : options.longs === Number ? new $util.LongBits(message.cnpj_socio.low >>> 0, message.cnpj_socio.high >>> 0).toNumber(true) : message.cnpj_socio;
                if (message.qualificacao_codigo != null && message.hasOwnProperty("qualificacao_codigo"))
                    object.qualificacao_codigo = message.qualificacao_codigo;
                if (message.data_entrada_sociedade != null && message.hasOwnProperty("data_entrada_sociedade"))
                    object.data_entrada_sociedade = message.data_entrada_sociedade;
                if (message.pais_codigo != null && message.hasOwnProperty("pais_codigo"))
                    object.pais_codigo = message.pais_codigo;
                if (message.faixa_etaria != null && message.hasOwnProperty("faixa_etaria"))
                    object.faixa_etaria = options.enums === String ? $root.ficha.v1.FaixaEtaria[message.faixa_etaria] === undefined ? message.faixa_etaria : $root.ficha.v1.FaixaEtaria[message.faixa_etaria] : message.faixa_etaria;
                if (message.representante_legal_cpf_meio != null && message.hasOwnProperty("representante_legal_cpf_meio"))
                    object.representante_legal_cpf_meio = message.representante_legal_cpf_meio;
                if (message.representante_legal_nome != null && message.hasOwnProperty("representante_legal_nome"))
                    object.representante_legal_nome = message.representante_legal_nome;
                if (message.representante_legal_qualificacao_codigo != null && message.hasOwnProperty("representante_legal_qualificacao_codigo"))
                    object.representante_legal_qualificacao_codigo = message.representante_legal_qualificacao_codigo;
                return object;
            };

            /**
             * Converts this Socio to JSON.
             * @function toJSON
             * @memberof ficha.v1.Socio
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            Socio.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            /**
             * Gets the type url for Socio
             * @function getTypeUrl
             * @memberof ficha.v1.Socio
             * @static
             * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns {string} The type url
             */
            Socio.getTypeUrl = function getTypeUrl(prefix) {
                if (prefix === undefined)
                    prefix = "type.googleapis.com";
                return prefix + "/ficha.v1.Socio";
            };

            return Socio;
        })();

        v1.LookupEntry = (function() {

            /**
             * Properties of a LookupEntry.
             * @typedef {Object} ficha.v1.LookupEntry.$Properties
             * @property {number|null} [codigo] LookupEntry codigo
             * @property {string|null} [descricao] LookupEntry descricao
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */

            /**
             * Properties of a LookupEntry.
             * @memberof ficha.v1
             * @interface ILookupEntry
             * @augments ficha.v1.LookupEntry.$Properties
             * @deprecated Use ficha.v1.LookupEntry.$Properties instead.
             */

            /**
             * Shape of a LookupEntry.
             * @typedef {ficha.v1.LookupEntry.$Properties} ficha.v1.LookupEntry.$Shape
             */

            /**
             * Constructs a new LookupEntry.
             * @memberof ficha.v1
             * @classdesc Represents a LookupEntry.
             * @constructor
             * @param {ficha.v1.LookupEntry.$Properties=} [properties] Properties to set
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */
            function LookupEntry(properties) {
                if (properties)
                    for (let keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null && keys[i] !== "__proto__")
                            this[keys[i]] = properties[keys[i]];
            }

            /**
             * LookupEntry codigo.
             * @member {number} codigo
             * @memberof ficha.v1.LookupEntry
             * @instance
             */
            LookupEntry.prototype.codigo = 0;

            /**
             * LookupEntry descricao.
             * @member {string} descricao
             * @memberof ficha.v1.LookupEntry
             * @instance
             */
            LookupEntry.prototype.descricao = "";

            /**
             * Creates a new LookupEntry instance using the specified properties.
             * @function create
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {ficha.v1.LookupEntry.$Properties=} [properties] Properties to set
             * @returns {ficha.v1.LookupEntry} LookupEntry instance
             * @type {{
             *   (properties: ficha.v1.LookupEntry.$Shape): ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape;
             *   (properties?: ficha.v1.LookupEntry.$Properties): ficha.v1.LookupEntry;
             * }}
             */
            LookupEntry.create = function create(properties) {
                return new LookupEntry(properties);
            };

            /**
             * Encodes the specified LookupEntry message. Does not implicitly {@link ficha.v1.LookupEntry.verify|verify} messages.
             * @function encode
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {ficha.v1.LookupEntry.$Properties} message LookupEntry message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            LookupEntry.encode = function encode(message, writer) {
                if (!writer)
                    writer = $Writer.create();
                if (message.codigo != null && Object.hasOwnProperty.call(message, "codigo"))
                    writer.uint32(/* id 1, wireType 0 =*/8).uint32(message.codigo);
                if (message.descricao != null && Object.hasOwnProperty.call(message, "descricao"))
                    writer.uint32(/* id 2, wireType 2 =*/18).string(message.descricao);
                if (message.$unknowns != null && Object.hasOwnProperty.call(message, "$unknowns"))
                    for (let i = 0; i < message.$unknowns.length; ++i)
                        writer.raw(message.$unknowns[i]);
                return writer;
            };

            /**
             * Encodes the specified LookupEntry message, length delimited. Does not implicitly {@link ficha.v1.LookupEntry.verify|verify} messages.
             * @function encodeDelimited
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {ficha.v1.LookupEntry.$Properties} message LookupEntry message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            LookupEntry.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a LookupEntry message from the specified reader or buffer.
             * @function decode
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape} LookupEntry
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            LookupEntry.decode = function decode(reader, length, _end, _depth, _target) {
                if (!(reader instanceof $Reader))
                    reader = $Reader.create(reader);
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $Reader.recursionLimit)
                    throw Error("max depth exceeded");
                let end = length === undefined ? reader.len : reader.pos + length, message = _target || new $root.ficha.v1.LookupEntry(), value;
                while (reader.pos < end) {
                    let start = reader.pos;
                    let tag = reader.tag();
                    if (tag === _end) {
                        _end = undefined;
                        break;
                    }
                    let wireType = tag & 7;
                    switch (tag >>>= 3) {
                    case 1: {
                            if (wireType !== 0)
                                break;
                            if (value = reader.uint32())
                                message.codigo = value;
                            else
                                delete message.codigo;
                            continue;
                        }
                    case 2: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.descricao = value;
                            else
                                delete message.descricao;
                            continue;
                        }
                    }
                    reader.skipType(wireType, _depth, tag);
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
                if (_end !== undefined)
                    throw Error("missing end group");
                return message;
            };

            /**
             * Decodes a LookupEntry message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {ficha.v1.LookupEntry & ficha.v1.LookupEntry.$Shape} LookupEntry
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            LookupEntry.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader))
                    reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a LookupEntry message.
             * @function verify
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            LookupEntry.verify = function verify(message, _depth) {
                if (typeof message !== "object" || message === null)
                    return "object expected";
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    return "max depth exceeded";
                if (message.codigo != null && message.hasOwnProperty("codigo"))
                    if (!$util.isInteger(message.codigo))
                        return "codigo: integer expected";
                if (message.descricao != null && message.hasOwnProperty("descricao"))
                    if (!$util.isString(message.descricao))
                        return "descricao: string expected";
                return null;
            };

            /**
             * Creates a LookupEntry message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {ficha.v1.LookupEntry} LookupEntry
             */
            LookupEntry.fromObject = function fromObject(object, _depth) {
                if (object instanceof $root.ficha.v1.LookupEntry)
                    return object;
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    throw Error("max depth exceeded");
                let message = new $root.ficha.v1.LookupEntry();
                if (object.codigo != null)
                    if (Number(object.codigo) !== 0)
                        message.codigo = object.codigo >>> 0;
                if (object.descricao != null)
                    if (typeof object.descricao !== "string" || object.descricao.length)
                        message.descricao = String(object.descricao);
                return message;
            };

            /**
             * Creates a plain object from a LookupEntry message. Also converts values to other types if specified.
             * @function toObject
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {ficha.v1.LookupEntry} message LookupEntry
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            LookupEntry.toObject = function toObject(message, options) {
                if (!options)
                    options = {};
                let object = {};
                if (options.defaults) {
                    object.codigo = 0;
                    object.descricao = "";
                }
                if (message.codigo != null && message.hasOwnProperty("codigo"))
                    object.codigo = message.codigo;
                if (message.descricao != null && message.hasOwnProperty("descricao"))
                    object.descricao = message.descricao;
                return object;
            };

            /**
             * Converts this LookupEntry to JSON.
             * @function toJSON
             * @memberof ficha.v1.LookupEntry
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            LookupEntry.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            /**
             * Gets the type url for LookupEntry
             * @function getTypeUrl
             * @memberof ficha.v1.LookupEntry
             * @static
             * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns {string} The type url
             */
            LookupEntry.getTypeUrl = function getTypeUrl(prefix) {
                if (prefix === undefined)
                    prefix = "type.googleapis.com";
                return prefix + "/ficha.v1.LookupEntry";
            };

            return LookupEntry;
        })();

        v1.LookupFile = (function() {

            /**
             * Properties of a LookupFile.
             * @typedef {Object} ficha.v1.LookupFile.$Properties
             * @property {string|null} [kind] LookupFile kind
             * @property {Array.<ficha.v1.LookupEntry.$Properties>|null} [entries] LookupFile entries
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */

            /**
             * Properties of a LookupFile.
             * @memberof ficha.v1
             * @interface ILookupFile
             * @augments ficha.v1.LookupFile.$Properties
             * @deprecated Use ficha.v1.LookupFile.$Properties instead.
             */

            /**
             * Shape of a LookupFile.
             * @typedef {ficha.v1.LookupFile.$Properties} ficha.v1.LookupFile.$Shape
             */

            /**
             * Constructs a new LookupFile.
             * @memberof ficha.v1
             * @classdesc Represents a LookupFile.
             * @constructor
             * @param {ficha.v1.LookupFile.$Properties=} [properties] Properties to set
             * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding
             */
            function LookupFile(properties) {
                this.entries = [];
                if (properties)
                    for (let keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null && keys[i] !== "__proto__")
                            this[keys[i]] = properties[keys[i]];
            }

            /**
             * LookupFile kind.
             * @member {string} kind
             * @memberof ficha.v1.LookupFile
             * @instance
             */
            LookupFile.prototype.kind = "";

            /**
             * LookupFile entries.
             * @member {Array.<ficha.v1.LookupEntry.$Properties>} entries
             * @memberof ficha.v1.LookupFile
             * @instance
             */
            LookupFile.prototype.entries = $util.emptyArray;

            /**
             * Creates a new LookupFile instance using the specified properties.
             * @function create
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {ficha.v1.LookupFile.$Properties=} [properties] Properties to set
             * @returns {ficha.v1.LookupFile} LookupFile instance
             * @type {{
             *   (properties: ficha.v1.LookupFile.$Shape): ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape;
             *   (properties?: ficha.v1.LookupFile.$Properties): ficha.v1.LookupFile;
             * }}
             */
            LookupFile.create = function create(properties) {
                return new LookupFile(properties);
            };

            /**
             * Encodes the specified LookupFile message. Does not implicitly {@link ficha.v1.LookupFile.verify|verify} messages.
             * @function encode
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {ficha.v1.LookupFile.$Properties} message LookupFile message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            LookupFile.encode = function encode(message, writer) {
                if (!writer)
                    writer = $Writer.create();
                if (message.kind != null && Object.hasOwnProperty.call(message, "kind"))
                    writer.uint32(/* id 1, wireType 2 =*/10).string(message.kind);
                if (message.entries != null && message.entries.length)
                    for (let i = 0; i < message.entries.length; ++i)
                        $root.ficha.v1.LookupEntry.encode(message.entries[i], writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
                if (message.$unknowns != null && Object.hasOwnProperty.call(message, "$unknowns"))
                    for (let i = 0; i < message.$unknowns.length; ++i)
                        writer.raw(message.$unknowns[i]);
                return writer;
            };

            /**
             * Encodes the specified LookupFile message, length delimited. Does not implicitly {@link ficha.v1.LookupFile.verify|verify} messages.
             * @function encodeDelimited
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {ficha.v1.LookupFile.$Properties} message LookupFile message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            LookupFile.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a LookupFile message from the specified reader or buffer.
             * @function decode
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape} LookupFile
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            LookupFile.decode = function decode(reader, length, _end, _depth, _target) {
                if (!(reader instanceof $Reader))
                    reader = $Reader.create(reader);
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $Reader.recursionLimit)
                    throw Error("max depth exceeded");
                let end = length === undefined ? reader.len : reader.pos + length, message = _target || new $root.ficha.v1.LookupFile(), value;
                while (reader.pos < end) {
                    let start = reader.pos;
                    let tag = reader.tag();
                    if (tag === _end) {
                        _end = undefined;
                        break;
                    }
                    let wireType = tag & 7;
                    switch (tag >>>= 3) {
                    case 1: {
                            if (wireType !== 2)
                                break;
                            if ((value = reader.string()).length)
                                message.kind = value;
                            else
                                delete message.kind;
                            continue;
                        }
                    case 2: {
                            if (wireType !== 2)
                                break;
                            if (!(message.entries && message.entries.length))
                                message.entries = [];
                            message.entries.push($root.ficha.v1.LookupEntry.decode(reader, reader.uint32(), undefined, _depth + 1));
                            continue;
                        }
                    }
                    reader.skipType(wireType, _depth, tag);
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
                if (_end !== undefined)
                    throw Error("missing end group");
                return message;
            };

            /**
             * Decodes a LookupFile message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {ficha.v1.LookupFile & ficha.v1.LookupFile.$Shape} LookupFile
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            LookupFile.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader))
                    reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a LookupFile message.
             * @function verify
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            LookupFile.verify = function verify(message, _depth) {
                if (typeof message !== "object" || message === null)
                    return "object expected";
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    return "max depth exceeded";
                if (message.kind != null && message.hasOwnProperty("kind"))
                    if (!$util.isString(message.kind))
                        return "kind: string expected";
                if (message.entries != null && message.hasOwnProperty("entries")) {
                    if (!Array.isArray(message.entries))
                        return "entries: array expected";
                    for (let i = 0; i < message.entries.length; ++i) {
                        let error = $root.ficha.v1.LookupEntry.verify(message.entries[i], _depth + 1);
                        if (error)
                            return "entries." + error;
                    }
                }
                return null;
            };

            /**
             * Creates a LookupFile message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {ficha.v1.LookupFile} LookupFile
             */
            LookupFile.fromObject = function fromObject(object, _depth) {
                if (object instanceof $root.ficha.v1.LookupFile)
                    return object;
                if (_depth === undefined)
                    _depth = 0;
                if (_depth > $util.recursionLimit)
                    throw Error("max depth exceeded");
                let message = new $root.ficha.v1.LookupFile();
                if (object.kind != null)
                    if (typeof object.kind !== "string" || object.kind.length)
                        message.kind = String(object.kind);
                if (object.entries) {
                    if (!Array.isArray(object.entries))
                        throw TypeError(".ficha.v1.LookupFile.entries: array expected");
                    message.entries = Array(object.entries.length);
                    for (let i = 0; i < object.entries.length; ++i) {
                        if (typeof object.entries[i] !== "object")
                            throw TypeError(".ficha.v1.LookupFile.entries: object expected");
                        message.entries[i] = $root.ficha.v1.LookupEntry.fromObject(object.entries[i], _depth + 1);
                    }
                }
                return message;
            };

            /**
             * Creates a plain object from a LookupFile message. Also converts values to other types if specified.
             * @function toObject
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {ficha.v1.LookupFile} message LookupFile
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            LookupFile.toObject = function toObject(message, options) {
                if (!options)
                    options = {};
                let object = {};
                if (options.arrays || options.defaults)
                    object.entries = [];
                if (options.defaults)
                    object.kind = "";
                if (message.kind != null && message.hasOwnProperty("kind"))
                    object.kind = message.kind;
                if (message.entries && message.entries.length) {
                    object.entries = Array(message.entries.length);
                    for (let j = 0; j < message.entries.length; ++j)
                        object.entries[j] = $root.ficha.v1.LookupEntry.toObject(message.entries[j], options);
                }
                return object;
            };

            /**
             * Converts this LookupFile to JSON.
             * @function toJSON
             * @memberof ficha.v1.LookupFile
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            LookupFile.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            /**
             * Gets the type url for LookupFile
             * @function getTypeUrl
             * @memberof ficha.v1.LookupFile
             * @static
             * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
             * @returns {string} The type url
             */
            LookupFile.getTypeUrl = function getTypeUrl(prefix) {
                if (prefix === undefined)
                    prefix = "type.googleapis.com";
                return prefix + "/ficha.v1.LookupFile";
            };

            return LookupFile;
        })();

        return v1;
    })();

    return ficha;
})();

export {
  $root as default
};
