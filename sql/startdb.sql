-- A partir do layout [https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/novolayoutdosdadosabertosdocnpj-dez2021.pdf]

-- Para executar via linha de comando shell/bash:
	-- psql -U postgres -d qd_receita -h localhost -f caminho/nomedoarquivo.sql

-- Gerenciar as operações relativas às Empresas (CNPJ)
drop table if exists empresa;
create table empresa (
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ).
	cnpj TEXT PRIMARY KEY,
	-- NOME EMPRESARIAL DA PESSOA JURÍDICA
	razao_social TEXT,
	-- CÓDIGO DA NATUREZA JURÍDICA
	codigo_natureza_juridica INT,
	-- QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA
	qualificacao_do_responsavel INT,
	-- CAPITAL SOCIAL DA EMPRESA
	capital_social REAL,
	-- CÓDIGO DO PORTE DA EMPRESA:
		-- 00 - NÃO INFORMADO
		-- 01 - MICRO EMPRESA
		-- 03 - EMPRESA DE PEQUENO PORTE
		-- 05 - DEMAIS
	porte INT,
	-- O ENTE FEDERATIVO RESPONSÁVEL É PREENCHIDO PARA OS CASOS DE ÓRGÃOS E ENTIDADES DO GRUPO DE NATUREZA JURÍDICA 1XXX.
	-- PARA AS DEMAIS NATUREZAS, ESTE ATRIBUTO FICA EM BRANCO.
	ente_federativo_responsavel TEXT
);

-- Gerenciar as operações relativas aos Estabelecimentos (CNPJ)
drop table if exists estabelecimento;
create table estabelecimento (
	-- Chave primária
	cnpj TEXT,
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ).
	cnpj_basico TEXT,
	-- NÚMERO DO ESTABELECIMENTO DE INSCRIÇÃO NO CNPJ (DO NONO ATÉ O DÉCIMO SEGUNDO DÍGITO DO CNPJ).
	cnpj_ordem TEXT,
	-- DÍGITO VERIFICADOR DO NÚMERO DE INSCRIÇÃO NO CNPJ (DOIS ÚLTIMOS DÍGITOS DO CNPJ).
	cnpj_dv INT,
	-- CÓDIGO DO IDENTIFICADOR MATRIZ/FILIAL:
		-- 1 - MATRIZ
		-- 2 - FILIAL
	identificador_matriz_filial INT,
	-- CORRESPONDE AO NOME FANTASIA
	nome_fantasia TEXT,
	-- CÓDIGO DA SITUAÇÃO CADASTRAL:
		-- 01 - NULA
		-- 2 - ATIVA
		-- 3 - SUSPENSA
		-- 4 - INAPTA
		-- 08 - BAIXADA
	situacao_cadastral INT,
	-- DATA DO EVENTO DA SITUAÇÃO CADASTRAL
	data_situacao_cadastral TEXT,
	-- CÓDIGO DO MOTIVO DA SITUAÇÃO CADASTRAL
	motivo_situacao_cadastral INT,
	-- NOME DA CIDADE NO EXTERIOR
	nome_cidade_exterior TEXT,
	-- CÓDIGO DO PAIS
	pais INT,
	-- DATA DE INÍCIO DA ATIVIDADE
	data_inicio_atividade TEXT,
	-- CÓDIGO DA ATIVIDADE ECONÔMICA PRINCIPAL DO ESTABELECIMENTO
	cnae_fiscal INT,
	-- CÓDIGO DA(S) ATIVIDADE(S) ECONÔMICA(S) SECUNDÁRIA(S) DO ESTABELECIMENTO
	cnae_fiscal_secundario INT,
	-- DESCRIÇÃO DO TIPO DE LOGRADOURO
	tipo_logradouro TEXT,
	-- NOME DO LOGRADOURO ONDE SE LOCALIZA O ESTABELECIMENTO.
	logradouro TEXT,
	-- NÚMERO ONDE SE LOCALIZA O ESTABELECIMENTO. QUANDO NÃO HOUVER PREENCHIMENTO DO NÚMERO HAVERÁ ‘S/N’.
	numero TEXT,
	-- COMPLEMENTO PARA O ENDEREÇO DE LOCALIZAÇÃO DO ESTABELECIMENTO
	complemento TEXT,
	-- BAIRRO ONDE SE LOCALIZA O ESTABELECIMENTO.
	bairro TEXT,
	-- CÓDIGO DE ENDEREÇAMENTO POSTAL REFERENTE AO LOGRADOURO NO QUAL O ESTABELECIMENTO ESTA LOCALIZADO
	cep INT,
	-- SIGLA DA UNIDADE DA FEDERAÇÃO EM QUE SE ENCONTRA O ESTABELECIMENTO
	uf TEXT,
	-- CÓDIGO DO MUNICÍPIO DE JURISDIÇÃO ONDE SE ENCONTRA O ESTABELECIMENTO
	municipio TEXT,
	-- CONTÉM O DDD 1
	ddd_1 VARCHAR(2),
	-- CONTÉM O NÚMERO DO TELEFONE 1
	telefone_1 VARCHAR(9),
	-- CONTÉM O DDD 2
	ddd_2 varchar(2),
	-- CONTÉM O NÚMERO DO TELEFONE 2
	telefone_2 VARCHAR(9),
	-- CONTÉM O DDD DO FAX
	ddd_fax VARCHAR(2),
	-- CONTÉM O NÚMERO DO FAX
	telefone_fax VARCHAR(9),
	-- CONTÉM O E-MAIL DO CONTRIBUINTE
	correio_eletronico TEXT,
	-- SITUAÇÃO ESPECIAL DA EMPRESA
	situacao_especial TEXT,
	-- DATA EM QUE A EMPRESA ENTROU EM SITUAÇÃO ESPECIAL
	data_situacao_especial TEXT
);

-- Gerenciar os Dados do Simples Nacional
drop table if exists simples;
create table simples (
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ).
	cnpj_basico TEXT,
	-- INDICADOR DA EXISTÊNCIA DA OPÇÃO PELO SIMPLES.
		-- S - SIM
		-- N - NÃO
		-- EM BRANCO - OUTROS
	opcao_pelo_simples VARCHAR(1),
	-- DATA DE OPÇÃO PELO SIMPLES
	data_opcao_pelo_simples TEXT,
	-- DATA DE EXCLUSÃO DO SIMPLES
	data_exclusao_pelo_simples TEXT,
	-- INDICADOR DA EXISTÊNCIA DA OPÇÃO PELO MEI
		-- S - SIM
		-- N - NÃO
		-- EM BRANCO - OUTROS
	opcao_pelo_mei VARCHAR(1),
	-- DATA DE OPÇÃO PELO MEI
	data_opcao_pelo_mei TEXT,
	-- DATA DE EXCLUSÃO DO MEI
	data_exclusao_pelo_mei TEXT
);

-- Gerenciar as operações relativas a Sócios
drop table if exists socio;
create table socio (
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA).
	cnpj_basico TEXT,
	-- CÓDIGO DO IDENTIFICADOR DE SÓCIO
		-- 1 - PESSOA JURÍDICA
		-- 2 - PESSOA FÍSICA
		-- 3 - ESTRANGEIRO
	identificador_socio INT,
	-- NOME DO SÓCIO PESSOA FÍSICA OU A RAZÃO SOCIAL E/OU NOME EMPRESARIAL DA PESSOA JURÍDICA
	-- E/OU NOME DO SÓCIO /RAZÃO SOCIAL DO SÓCIO ESTRANGEIRO
	razao_social TEXT,
	-- CPF OU CNPJ DO SÓCIO (SÓCIO ESTRANGEIRO NÃO TEM ESTA INFORMAÇÃO).
	cnpj_cpf_socio TEXT,
	-- CÓDIGO DA QUALIFICAÇÃO DO SÓCIO
	codigo_qualificacao_socio INT,
	-- DATA DE ENTRADA NA SOCIEDADE
	data_entrada_sociedade TEXT,
	-- CÓDIGO PAÍS DO SÓCIO ESTRANGEIRO
	codigo_pais_socio_estrangeiro INT,
	-- NÚMERO DO CPF DO REPRESENTANTE LEGAL
	numero_cpf_representante_legal INT,
	-- NOME DO REPRESENTANTE LEGAL
	nome_representante_legal TEXT,
	-- CÓDIGO DA QUALIFICAÇÃO DO REPRESENTANTE LEGAL
	codigo_qualificacao_representante_legal INT,
	-- CÓDIGO CORRESPONDENTE À FAIXA ETÁRIA DO SÓCIO
		-- 0 para não se aplica.
		-- 1 para os intervalos entre 0 a 12 anos
		-- 2 para os intervalos entre 13 a 20 anos
		-- 3 para os intervalos entre 21 a 30 anos
		-- 4 para os intervalos entre 31 a 40 anos
		-- 5 para os intervalos entre 41 a 50 anos
		-- 6 para os intervalos entre 51 a 60 anos
		-- 7 para os intervalos entre 61 a 70 anos
		-- 8 para os intervalos entre 71 a 80 anos
		-- 9 para maiores de 80 anos.
	faixa_etaria INT
);

-----------------------------------------------------------------------------------------------------------------
-- TABELAS DE DOMÍNIO

-- Gerenciar as operações relativas aos países
drop table if exists pais;
create table pais (
	-- CÓDIGO DO PAÍS
	codigo INT,
	-- NOME DO PAÍS
	descricao TEXT
);

-- Gerenciar as operações relativas aos países
drop table if exists municipio;
create table municipio (
	-- CÓDIGO DO MUNICÍPIO
	codigo INT,
	-- NOME DO MUNICÍPIO
	descricao TEXT
);

-- Gerenciar as operações relativas às qualificações dos sócios
drop table if exists qualificacao_socio;
create table qualificacao_socio (
	-- CÓDIGO DA QUALIFICAÇÃO DO SÓCIO
	codigo INT,
	-- NOME DA QUALIFICAÇÃO DO SÓCIO
	descricao TEXT
);

-- Gerenciar as operações relativas às naturezas jurídicas
drop table if exists natureza_juridica;
create table natureza_juridica (
	-- CÓDIGO DA NATUREZA JURÍDICA
	codigo INT,
	-- NOME DA NATUREZA JURÍDICA
	descricao TEXT
);

-- Gerenciar as operações relativas às Empresas (CNAE)
drop table if exists cnae;
create table cnae (
	-- CÓDIGO DA ATIVIDADE ECONÔMICA
	codigo INT,
	-- NOME DA ATIVIDADE ECONÔMICA
	descricao TEXT
);

-- Primary keys:  

ALTER TABLE IF EXISTS empresa DROP CONSTRAINT IF EXISTS pk_empresa_id;
ALTER TABLE empresa ADD CONSTRAINT pk_empresa_id PRIMARY KEY (cnpj);
ALTER TABLE IF EXISTS cnae DROP CONSTRAINT IF EXISTS pk_cnae_codigo;
ALTER TABLE cnae ADD CONSTRAINT pk_cnae_codigo PRIMARY KEY (codigo);
ALTER TABLE IF EXISTS estabelecimento DROP CONSTRAINT IF EXISTS pk_estabelecimento_id;
ALTER TABLE estabelecimento ADD CONSTRAINT pk_estabelecimento_id PRIMARY KEY (cnpj);
ALTER TABLE IF EXISTS simples DROP CONSTRAINT IF EXISTS pk_simples_id;
ALTER TABLE simples ADD CONSTRAINT pk_simples_id PRIMARY KEY (cnpj_basico);
ALTER TABLE IF EXISTS socio DROP CONSTRAINT IF EXISTS pk_socio_id;
ALTER TABLE socio ADD CONSTRAINT pk_socio_id PRIMARY KEY (cnpj_basico);

-- Foreign Keys:
-- ALTER TABLE cnae_cnpj ADD CONSTRAINT fk_cnae_cnpj_cnpj FOREIGN KEY (cnpj) REFERENCES empresa (cnpj);

-- Índices:
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_cnpj ON empresa (cnpj);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cnae_codigo ON cnae (codigo);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cnae_descricao ON cnae (descricao);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimento (cnpj);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_cnpj_basico ON simples (cnpj_basico);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socio_cnpj_basico ON socio (cnpj_basico);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnae_fiscal ON estabelecimento (cnae_fiscal);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_local ON estabelecimento (uf, municipio, bairro);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_situacao_cadastral ON estabelecimento (situacao_cadastral);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_mei_simples ON simples (opcao_pelo_mei, opcao_pelo_simples);

-- #Review and adjust with or without usage of pg_trgm!
--CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- PostgreSQL-only
--CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trgm_socio_nome_socio ON socio USING gin (nome_socio gin_trgm_ops);
--CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trgm_empresa_razao_social ON empresa USING gin (razao_social gin_trgm_ops);
--CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trgm_empresa_nome_fantasia ON empresa USING gin (nome_fantasia gin_trgm_ops);
--CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trgm_cnae_descricao ON cnae USING gin (descricao_subclasse gin_trgm_ops);
--CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trgm_empresa_uf ON empresa USING gin (uf gin_trgm_ops);
--CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trgm_empresa_municipio ON empresa USING gin (municipio gin_trgm_ops);
--CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trgm_empresa_bairro ON empresa USING gin (bairro gin_trgm_ops);
