-- A partir do layout [https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/novolayoutdosdadosabertosdocnpj-dez2021.pdf]

-- Para executar via linha de comando shell/bash:
	-- psql -U postgres -d qd_receita -h localhost -f caminho/nomedoarquivo.sql

-- Gerenciar as operações relativas às Empresas (CNPJ)
drop table if exists empresa;
create table empresa (
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ).
	cnpj VARCHAR(15) PRIMARY KEY,
	-- NOME EMPRESARIAL DA PESSOA JURÍDICA
	razao_social VARCHAR,
	-- CÓDIGO DA NATUREZA JURÍDICA
	codigo_natureza_juridica SMALLINT,
	-- QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA
	qualificacao_do_responsavel SMALLINT,
	-- CAPITAL SOCIAL DA EMPRESA
	capital_social VARCHAR,
	-- CÓDIGO DO PORTE DA EMPRESA:
		-- 00 - NÃO INFORMADO
		-- 01 - MICRO EMPRESA
		-- 03 - EMPRESA DE PEQUENO PORTE
		-- 05 - DEMAIS
	porte VARCHAR,
	-- O ENTE FEDERATIVO RESPONSÁVEL É PREENCHIDO PARA OS CASOS DE ÓRGÃOS E ENTIDADES DO GRUPO DE NATUREZA JURÍDICA.
	-- PARA AS DEMAIS NATUREZAS, ESTE ATRIBUTO FICA EM BRANCO.
	-- OBS.: Corresponde ao par cidade - uf
	ente_federativo_responsavel VARCHAR
);

-- Gerenciar as operações relativas aos Estabelecimentos (CNPJ)
drop table if exists estabelecimento;
create table estabelecimento (
	-- Chave primária
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ).
	cnpj_basico VARCHAR(8),
	-- NÚMERO DO ESTABELECIMENTO DE INSCRIÇÃO NO CNPJ (DO NONO ATÉ O DÉCIMO SEGUNDO DÍGITO DO CNPJ).
	cnpj_ordem VARCHAR(4),
	-- DÍGITO VERIFICADOR DO NÚMERO DE INSCRIÇÃO NO CNPJ (DOIS ÚLTIMOS DÍGITOS DO CNPJ).
	cnpj_dv VARCHAR(2),
	-- CÓDIGO DO IDENTIFICADOR MATRIZ/FILIAL:
		-- 1 - MATRIZ
		-- 2 - FILIAL
	identificador_matriz_filial CHAR,
	-- CORRESPONDE AO NOME FANTASIA
	nome_fantasia VARCHAR,
	-- CÓDIGO DA SITUAÇÃO CADASTRAL:
		-- 01 - NULA
		-- 2 - ATIVA
		-- 3 - SUSPENSA
		-- 4 - INAPTA
		-- 08 - BAIXADA
	situacao_cadastral VARCHAR(2),
	-- DATA DO EVENTO DA SITUAÇÃO CADASTRAL
	data_situacao_cadastral VARCHAR(10),
	-- CÓDIGO DO MOTIVO DA SITUAÇÃO CADASTRAL
	motivo_situacao_cadastral VARCHAR(11),
	-- NOME DA CIDADE NO EXTERIOR
	nome_cidade_exterior VARCHAR,
	-- CÓDIGO DO PAIS
	pais VARCHAR(10),
	-- DATA DE INÍCIO DA ATIVIDADE
	data_inicio_atividade VARCHAR(10),
	-- CÓDIGO DA ATIVIDADE ECONÔMICA PRINCIPAL DO ESTABELECIMENTO
	cnae_fiscal VARCHAR,
	-- CÓDIGO DA(S) ATIVIDADE(S) ECONÔMICA(S) SECUNDÁRIA(S) DO ESTABELECIMENTO
	cnae_fiscal_secundario VARCHAR,
	-- DESCRIÇÃO DO TIPO DE LOGRADOURO
	tipo_logradouro VARCHAR,
	-- NOME DO LOGRADOURO ONDE SE LOCALIZA O ESTABELECIMENTO.
	logradouro VARCHAR,
	-- NÚMERO ONDE SE LOCALIZA O ESTABELECIMENTO. QUANDO NÃO HOUVER PREENCHIMENTO DO NÚMERO HAVERÁ ‘S/N’.
	numero VARCHAR,
	-- COMPLEMENTO PARA O ENDEREÇO DE LOCALIZAÇÃO DO ESTABELECIMENTO
	complemento VARCHAR,
	-- BAIRRO ONDE SE LOCALIZA O ESTABELECIMENTO.
	bairro VARCHAR,
	-- CÓDIGO DE ENDEREÇAMENTO POSTAL REFERENTE AO LOGRADOURO NO QUAL O ESTABELECIMENTO ESTA LOCALIZADO
	cep VARCHAR(11),
	-- SIGLA DA UNIDADE DA FEDERAÇÃO EM QUE SE ENCONTRA O ESTABELECIMENTO
	uf VARCHAR(2),
	-- CÓDIGO DO MUNICÍPIO DE JURISDIÇÃO ONDE SE ENCONTRA O ESTABELECIMENTO
	municipio VARCHAR,
	-- CONTÉM O DDD 1
	ddd_1 VARCHAR(15),
	-- CONTÉM O NÚMERO DO TELEFONE 1
	telefone_1 VARCHAR(14),
	-- CONTÉM O DDD 2
	ddd_2 varchar(15),
	-- CONTÉM O NÚMERO DO TELEFONE 2
	telefone_2 VARCHAR(14),
	-- CONTÉM O DDD DO FAX
	ddd_fax VARCHAR(15),
	-- CONTÉM O NÚMERO DO FAX
	telefone_fax VARCHAR(14),
	-- CONTÉM O E-MAIL DO CONTRIBUINTE
	correio_eletronico VARCHAR,
	-- SITUAÇÃO ESPECIAL DA EMPRESA
	situacao_especial VARCHAR,
	-- DATA EM QUE A EMPRESA ENTROU EM SITUAÇÃO ESPECIAL
	data_situacao_especial VARCHAR(10)
);

-- Gerenciar os Dados do Simples Nacional
drop table if exists simples;
create table simples (
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ).
	cnpj_basico VARCHAR(8),
	-- INDICADOR DA EXISTÊNCIA DA OPÇÃO PELO SIMPLES.
		-- S - SIM
		-- N - NÃO
		-- EM BRANCO - OUTROS
	opcao_pelo_simples CHAR,
	-- DATA DE OPÇÃO PELO SIMPLES
	data_opcao_pelo_simples VARCHAR(10),
	-- DATA DE EXCLUSÃO DO SIMPLES
	data_exclusao_pelo_simples VARCHAR(10),
	-- INDICADOR DA EXISTÊNCIA DA OPÇÃO PELO MEI
		-- S - SIM
		-- N - NÃO
		-- EM BRANCO - OUTROS
	opcao_pelo_mei CHAR,
	-- DATA DE OPÇÃO PELO MEI
	data_opcao_pelo_mei VARCHAR(10),
	-- DATA DE EXCLUSÃO DO MEI
	data_exclusao_pelo_mei VARCHAR(10)
);

-- Gerenciar as operações relativas a Sócios
drop table if exists socio;
create table socio (
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA).
	cnpj_basico VARCHAR(8),
	-- CÓDIGO DO IDENTIFICADOR DE SÓCIO
		-- 1 - PESSOA JURÍDICA
		-- 2 - PESSOA FÍSICA
		-- 3 - ESTRANGEIRO
	identificador_socio VARCHAR(3),
	-- NOME DO SÓCIO PESSOA FÍSICA OU A RAZÃO SOCIAL E/OU NOME EMPRESARIAL DA PESSOA JURÍDICA
	-- E/OU NOME DO SÓCIO /RAZÃO SOCIAL DO SÓCIO ESTRANGEIRO
	razao_social VARCHAR,
	-- CPF OU CNPJ DO SÓCIO (SÓCIO ESTRANGEIRO NÃO TEM ESTA INFORMAÇÃO).
	cnpj_cpf_socio VARCHAR(15),
	-- CÓDIGO DA QUALIFICAÇÃO DO SÓCIO
	codigo_qualificacao_socio VARCHAR(3),
	-- DATA DE ENTRADA NA SOCIEDADE
	data_entrada_sociedade VARCHAR(10),
	-- CÓDIGO PAÍS DO SÓCIO ESTRANGEIRO
	codigo_pais_socio_estrangeiro VARCHAR(3),
	-- NÚMERO DO CPF DO REPRESENTANTE LEGAL
	numero_cpf_representante_legal VARCHAR(18),
	-- NOME DO REPRESENTANTE LEGAL
	nome_representante_legal VARCHAR,
	-- CÓDIGO DA QUALIFICAÇÃO DO REPRESENTANTE LEGAL
	codigo_qualificacao_representante_legal VARCHAR(3),
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
	faixa_etaria VARCHAR(10)
);

-----------------------------------------------------------------------------------------------------------------
-- TABELAS DE DOMÍNIO

-- Gerenciar as operações relativas aos países
drop table if exists pais;
create table pais (
	-- CÓDIGO DO PAÍS
	codigo SMALLINT,
	-- NOME DO PAÍS
	descricao VARCHAR
);

-- Gerenciar as operações relativas aos países
drop table if exists municipio;
create table municipio (
	-- CÓDIGO DO MUNICÍPIO
	codigo SMALLINT,
	-- NOME DO MUNICÍPIO
	descricao VARCHAR
);

-- Gerenciar as operações relativas às qualificações dos sócios
drop table if exists qualificacao_socio;
create table qualificacao_socio (
	-- CÓDIGO DA QUALIFICAÇÃO DO SÓCIO
	codigo SMALLINT,
	-- NOME DA QUALIFICAÇÃO DO SÓCIO
	descricao VARCHAR
);

-- Gerenciar as operações relativas às naturezas jurídicas
drop table if exists natureza_juridica;
create table natureza_juridica (
	-- CÓDIGO DA NATUREZA JURÍDICA
	codigo SMALLINT,
	-- NOME DA NATUREZA JURÍDICA
	descricao VARCHAR
);

-- Gerenciar as operações relativas às Empresas (CNAE)
drop table if exists cnae;
create table cnae (
	-- CÓDIGO DA ATIVIDADE ECONÔMICA
	codigo SMALLINT,
	-- NOME DA ATIVIDADE ECONÔMICA
	descricao VARCHAR
);

-- Tabela auxiliar com a resposta completa montada com referência aos códigos e descrição textual a ser retornada a partir de um CNPJ fornecido.
drop table if exists resposta_cnpj;
create table resposta_cnpj (
 	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ).
 	estabelecimento_cnpj_basico VARCHAR(8),
 	-- NÚMERO DO ESTABELECIMENTO DE INSCRIÇÃO NO CNPJ (DO NONO ATÉ O DÉCIMO SEGUNDO DÍGITO DO CNPJ).
 	estabelecimento_cnpj_ordem VARCHAR(4),
 	-- DÍGITO VERIFICADOR DO NÚMERO DE INSCRIÇÃO NO CNPJ (DOIS ÚLTIMOS DÍGITOS DO CNPJ).
 	estabelecimento_cnpj_dv VARCHAR(10),
	-- Chave primária é uma combinação das três colunas anteriores
	PRIMARY KEY(estabelecimento_cnpj_basico, estabelecimento_cnpj_ordemestabelecimento_cnpj_dv),
 	-- CÓDIGO DO IDENTIFICADOR MATRIZ/FILIAL:
 		-- '1 - MATRIZ'
 		-- '2 - FILIAL'
 	estabelecimento_identificador_matriz_filial VARCHAR,
 	-- CORRESPONDE AO NOME FANTASIA
 	estabelecimento_nome_fantasia VARCHAR,
 	-- CÓDIGO DA SITUAÇÃO CADASTRAL:
 		-- 01 - NULA
 		-- 2 - ATIVA
 		-- 3 - SUSPENSA
 		-- 4 - INAPTA
 		-- 08 - BAIXADA
 	estabelecimento_situacao_cadastral VARCHAR(15),
 	-- DATA DO EVENTO DA SITUAÇÃO CADASTRAL
 	estabelecimento_data_situacao_cadastral VARCHAR(10),
 	-- CÓDIGO DO MOTIVO DA SITUAÇÃO CADASTRAL
 	estabelecimento_motivo_situacao_cadastral VARCHAR(11),
 	-- NOME DA CIDADE NO EXTERIOR
 	estabelecimento_nome_cidade_exterior VARCHAR,
 	-- DATA DE INÍCIO DA ATIVIDADE
 	estabelecimento_data_inicio_atividade VARCHAR(10),
 	-- CÓDIGO DA(S) ATIVIDADE(S) ECONÔMICA(S) SECUNDÁRIA(S) DO ESTABELECIMENTO
 	estabelecimento_cnae_fiscal_secundario VARCHAR,
 	-- DESCRIÇÃO DO TIPO DE LOGRADOURO
 	estabelecimento_tipo_logradouro VARCHAR,
 	-- NOME DO LOGRADOURO ONDE SE LOCALIZA O ESTABELECIMENTO.
 	estabelecimento_logradouro VARCHAR,
 	-- NÚMERO ONDE SE LOCALIZA O ESTABELECIMENTO. QUANDO NÃO HOUVER PREENCHIMENTO DO NÚMERO HAVERÁ ‘S/N’.
 	estabelecimento_numero VARCHAR,
 	-- COMPLEMENTO PARA O ENDEREÇO DE LOCALIZAÇÃO DO ESTABELECIMENTO
 	estabelecimento_complemento VARCHAR,
 	-- BAIRRO ONDE SE LOCALIZA O ESTABELECIMENTO.
 	estabelecimento_bairro VARCHAR,
 	-- CÓDIGO DE ENDEREÇAMENTO POSTAL REFERENTE AO LOGRADOURO NO QUAL O ESTABELECIMENTO ESTA LOCALIZADO
 	estabelecimento_cep VARCHAR(11),
 	-- SIGLA DA UNIDADE DA FEDERAÇÃO EM QUE SE ENCONTRA O ESTABELECIMENTO
 	estabelecimento_uf VARCHAR(2),
 	-- CÓDIGO DO MUNICÍPIO DE JURISDIÇÃO ONDE SE ENCONTRA O ESTABELECIMENTO
 	estabelecimento_municipio VARCHAR,
 	-- CONTÉM O DDD 1 E O NÚMERO DO TELEFONE telefone_1
	estabelecimento_ddd_telefone_1 VARCHAR(16),
 	-- CONTÉM O DDD 2 E O NÚMERO DO TELEFONE telefone_2
 	estabelecimento_ddd_telefone_2 varchar(16),
 	-- CONTÉM O DDD E O NÚMERO DO FAX
 	estabelecimento_ddd_telefone_fax VARCHAR(16),
 	-- CONTÉM O E-MAIL DO CONTRIBUINTE
 	estabelecimento_correio_eletronico VARCHAR,
 	-- SITUAÇÃO ESPECIAL DA EMPRESA
 	estabelecimento_situacao_especial VARCHAR,
 	-- DATA EM QUE A EMPRESA ENTROU EM SITUAÇÃO ESPECIAL
 	estabelecimento_data_situacao_especial VARCHAR(10),

	-- Informações da tabela EMPRESA
	-- RAZÃO_SOCIAL DA EMPRESA
	empresa_razao_social VARCHAR,
	-- CÓDIGO DA NATUREZA JURÍDICA
	empresa_codigo_natureza_juridica SMALLINT,
	-- QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA
	empresa_qualificacao_do_responsavel SMALLINT,
	-- CAPITAL SOCIAL DA EMPRESA
	empresa_capital_social VARCHAR,
	-- CÓDIGO DO PORTE DA EMPRESA:
		-- 00 - NÃO INFORMADO
		-- 01 - MICRO EMPRESA
		-- 03 - EMPRESA DE PEQUENO PORTE
		-- 05 - DEMAIS
	empresa_porte VARCHAR,
	-- O ENTE FEDERATIVO RESPONSÁVEL É PREENCHIDO PARA OS CASOS DE ÓRGÃOS E ENTIDADES DO GRUPO DE NATUREZA JURÍDICA 1XXX.
	-- PARA AS DEMAIS NATUREZAS, ESTE ATRIBUTO FICA EM BRANCO.
	empresa_ente_federativo_responsavel VARCHAR,

	-- Informações tabela SIMPLES:
	-- INDICADOR DA EXISTÊNCIA DA OPÇÃO PELO SIMPLES.
		-- S - SIM
		-- N - NÃO
		-- EM BRANCO - OUTROS
	simples_opcao_pelo_simples VARCHAR(20),
	-- DATA DE OPÇÃO PELO SIMPLES
	simples_data_opcao_pelo_simples VARCHAR(10),
	-- DATA DE EXCLUSÃO DO SIMPLES
	simples_data_exclusao_pelo_simples VARCHAR(10),
	-- INDICADOR DA EXISTÊNCIA DA OPÇÃO PELO MEI
		-- S - SIM
		-- N - NÃO
		-- EM BRANCO - OUTROS
	simples_opcao_pelo_mei VARCHAR(20),
	-- DATA DE OPÇÃO PELO MEI
	simples_data_opcao_pelo_mei VARCHAR(10),
	-- DATA DE EXCLUSÃO DO MEI
	simples_data_exclusao_pelo_mei VARCHAR(10),

	-- cnae
	-- CÓDIGO DA ATIVIDADE ECONÔMICA PRINCIPAL DO ESTABELECIMENTO
	cnae VARCHAR,

	-- CÓDIGO E NOME DO PAÍS
	pais VARCHAR(50),
	-- CÓDIGO E NOME DO MUNICIPIO
	municipio VARCHAR(100)
);

-- Tabela para simplificar as relações de sociedade de modo que pelo CNPJ fornecido seja possível rapidamente encontrar todos os CNPJs que estão a ele associados
drop table if exists resposta_socios;
create table resposta_socios (
	-- NÚMERO BASE DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA) a ser buscado
	cnpj_basico VARCHAR(8) PRIMARY KEY,
	-- Sócios é a lista de CNPJs/CPFs relacionados ao CNPJ fornecido em texto e separados por vírgula
	socios VARCHAR
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
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimento (cnpj_basico);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_cnpj_basico ON simples (cnpj_basico);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socio_cnpj_basico ON socio (cnpj_basico);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnae_fiscal ON estabelecimento (cnae_fiscal);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_local ON estabelecimento (uf, municipio, bairro);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_situacao_cadastral ON estabelecimento (situacao_cadastral);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_mei_simples ON simples (opcao_pelo_mei, opcao_pelo_simples);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_resposta_cnpj ON resposta_cnpj (estabelecimento_cnpj_basico);
