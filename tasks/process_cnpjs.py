import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import extras
from querido_diario_toolbox.process.text_process import remove_breaks
import logging


# Retorna um objeto dicionário com uma chave para cada cnpj_basico e uma tupla  ordem, dv para cada cnpj_basico
def get_all_cnpj_ids(cursor=None, offset=0, limit=10000):
    resultados = {}
    logging.info(f"Offset: {offset} | Limit: {limit}")
    # Quando não tem limite, deve retornar tudo, portanto na consulta sem limites, deve ser omitido o parâmetro
    if limit == 0:
        sql = f"SELECT cnpj_basico, cnpj_ordem, cnpj_dv from estabelecimento order by cnpj_basico offset {offset}"
    # Executa uma consulta para pegar todos os identificadores únicos de CNPJ que serão utilizados nas buscas:
    else:
        sql = f"SELECT cnpj_basico, cnpj_ordem, cnpj_dv from estabelecimento order by cnpj_basico, cnpj_ordem, cnpj_dv limit {limit} offset {offset}"
    if cursor is not None and sql is not None:
        results = None
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except Exception as e:
            logging.error(f"Erro! SQL: {sql}")
            logging.error(e)
        if results is not None:
            for item in results:
                cb = item["cnpj_basico"]
                od = item["cnpj_ordem"]
                dv = item["cnpj_dv"]
                try:
                    verifica = resultados[cb]
                    # se já existe uma chave no dicionário para o cnpj básico, adiciona a tupla à lista do cnpj_basico
                    if verifica:
                        verifica.append((od, dv))
                    else:
                        resultados[cb].append((od, dv))
                # se não existe, insere uma tupla em uma nova lista para esta chave
                except Exception as e:
                    resultados[cb] = [(od, dv)]
            return resultados
        return None


def batch_insert_resposta_cnpj(cursor, resposta_cnpj_lista_valores):
    # Consulta adaptada para inserção via execute_batch
    sql_insert = "INSERT into resposta_cnpj (estabelecimento_cnpj_basico, empresa_razao_social, empresa_natureza_juridica, empresa_qualificacao_do_responsavel, empresa_capital_social, empresa_porte, empresa_ente_federativo_responsavel, simples_opcao_pelo_simples, simples_data_opcao_pelo_simples, simples_data_exclusao_pelo_simples, simples_opcao_pelo_mei, simples_data_opcao_pelo_mei, simples_data_exclusao_pelo_mei, estabelecimento_cnpj_ordem, estabelecimento_cnpj_dv, estabelecimento_identificador_matriz_filial, estabelecimento_nome_fantasia, estabelecimento_situacao_cadastral, estabelecimento_data_situacao_cadastral, estabelecimento_motivo_situacao_cadastral, estabelecimento_nome_cidade_exterior, estabelecimento_data_inicio_atividade, estabelecimento_cnae_fiscal_secundario, estabelecimento_tipo_logradouro, estabelecimento_logradouro, estabelecimento_numero, estabelecimento_complemento, estabelecimento_bairro, estabelecimento_cep, estabelecimento_uf, estabelecimento_ddd_telefone_1, estabelecimento_ddd_telefone_2, estabelecimento_ddd_telefone_fax, estabelecimento_correio_eletronico, estabelecimento_situacao_especial, estabelecimento_data_situacao_especial, cnae, pais, municipio)"
    sql_insert = (
        sql_insert
        + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    if cursor is not None and resposta_cnpj_lista_valores is not None:
        try:
            extras.execute_batch(cursor, sql_insert, resposta_cnpj_lista_valores)
            logging.info(
                f"Batch insert executado salvou {len(resposta_cnpj_lista_valores)} itens"
            )
        except Exception as e:
            logging.error("Erro de inserção batch: " + str(e))


def batch_insert_resposta_socios(cursor, lista_resposta_socios):
    # Consulta adaptada para inserção via execute_batch
    sql_insert = "INSERT into resposta_socios (cnpj_basico, identificador_socio, razao_social, cnpj_cpf_socio, qualificacao_socio, data_entrada_sociedade, pais_socio_estrangeiro, numero_cpf_representante_legal, nome_representante_legal, codigo_qualificacao_representante_legal, faixa_etaria) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    if cursor is not None and lista_resposta_socios is not None:
        try:
            extras.execute_batch(cursor, sql_insert, lista_resposta_socios)
        except Exception as e:
            logging.error("Erro de inserção batch: " + str(e))


# Processar a partir de um CNPJ as tabelas relacionadas à empresa e possíveis dados do simples
def process_resposta_cnpjs_empresa(cnpj_basico: str, cursor=None):
    # Executa uma consulta ao banco para retornar com join as informações das tabelas complementares e montar o registro a ser salvo na tabela resposta_cnpj
    sql = "SELECT empresa.cnpj as empresa_cnpj,"
    sql = sql + " empresa.razao_social as empresa_razao_social,"
    sql = sql + " empresa.codigo_natureza_juridica as empresa_codigo_natureza_juridica,"
    sql = (
        sql
        + " empresa.qualificacao_do_responsavel as empresa_qualificacao_do_responsavel,"
    )
    sql = sql + " empresa.capital_social as empresa_capital_social,"
    sql = sql + " empresa.porte as empresa_porte,"
    sql = (
        sql
        + " empresa.ente_federativo_responsavel as empresa_ente_federativo_responsavel"
    )
    sql = sql + " FROM empresa"
    sql = sql + f" WHERE empresa.cnpj =  '{cnpj_basico}';"

    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        # Armazena os campos a serem salvos na tabela de resposta_cnpj
        resposta_cnpj = {}
        campos_cnpj = {}
        if results is not None and results != []:
            r = results[0]
            campos_cnpj = r.copy()
            # Para as empresas que não contém estabelecimento, importante configurar o campo estabelecimento_cnpj_basico
            resposta_cnpj["estabelecimento_cnpj_basico"] = remove_breaks(cnpj_basico)
            resposta_cnpj["empresa_razao_social"] = remove_breaks(
                str(campos_cnpj["empresa_razao_social"]).replace("'", "`")
            )

            # Dados natureza_juridica
            empresa_codigo_natureza_juridica = remove_breaks(
                campos_cnpj["empresa_codigo_natureza_juridica"]
            )
            # Realizar consulta complementar para a natureza jurídica da empresa:
            sql_natureza_juridica = f"SELECT * from natureza_juridica where codigo = '{empresa_codigo_natureza_juridica}'"
            cursor.execute(sql_natureza_juridica)
            results = cursor.fetchall()
            if results is not None and results != []:
                campos_natureza_juridica = results[0]
                if campos_natureza_juridica is None:
                    resposta_cnpj["empresa_natureza_juridica"] = None
                else:
                    resposta_cnpj["empresa_natureza_juridica"] = (
                        str(empresa_codigo_natureza_juridica)
                        + " - "
                        + campos_natureza_juridica["descricao"]
                    )

            # Implementar select a partir da tabela qualificacao_socio
            empresa_qualificacao_do_responsavel = remove_breaks(
                campos_cnpj["empresa_qualificacao_do_responsavel"]
            )
            if (
                empresa_qualificacao_do_responsavel is not None
                and empresa_qualificacao_do_responsavel != ""
            ):
                # SQL a ser realizada para buscar as informações da qualificação do responsável
                sql = f"select * from qualificacao_socio where codigo = '{empresa_qualificacao_do_responsavel}'"
                cursor.execute(sql)
                results = cursor.fetchall()
                if results is not None and results != []:
                    # Should be a single result, fetch first item
                    campos = results[0]
                    if campos is not None:
                        codigo = str(campos["codigo"])
                        descricao = str(campos["descricao"])
                        resposta_cnpj[
                            "empresa_qualificacao_do_responsavel"
                        ] = f"{codigo} - {descricao}"
                    else:
                        resposta_cnpj[
                            "empresa_qualificacao_do_responsavel"
                        ] = empresa_qualificacao_do_responsavel
                else:
                    logging.error(
                        f"Erro! Porte da empresa {empresa_qualificacao_do_responsavel} não encontrado"
                    )
                    resposta_cnpj[
                        "empresa_qualificacao_do_responsavel"
                    ] = empresa_qualificacao_do_responsavel
            else:
                resposta_cnpj["empresa_qualificacao_do_responsavel"] = None

            resposta_cnpj["empresa_capital_social"] = remove_breaks(
                campos_cnpj["empresa_capital_social"]
            )

            # Implement select from dim_porte_empresa
            empresa_porte = remove_breaks(str(campos_cnpj["empresa_porte"]))
            if empresa_porte is not None and empresa_porte != "":
                # SQL a ser realizada para buscar as informações do porte
                sql = (
                    f"select * from dim_porte_empresa where codigo = '{empresa_porte}'"
                )
                cursor.execute(sql)
                results = cursor.fetchall()
                if results is not None and results != []:
                    # Should be a single result, fetch first item
                    campos = results[0]
                    if campos is not None:
                        codigo = str(campos["codigo"])
                        descricao = str(campos["descricao"])
                        resposta_cnpj["empresa_porte"] = f"{codigo} - {descricao}"
                    else:
                        resposta_cnpj["empresa_porte"] = empresa_porte
                else:
                    logging.error(
                        f"Erro! Porte da empresa {empresa_porte} não encontrado"
                    )
                    resposta_cnpj["empresa_porte"] = empresa_porte
            else:
                resposta_cnpj["empresa_porte"] = None

            empresa_ente_federativo_responsavel = campos_cnpj[
                "empresa_ente_federativo_responsavel"
            ]
            if empresa_ente_federativo_responsavel is not None:
                empresa_ente_federativo_responsavel = remove_breaks(
                    empresa_ente_federativo_responsavel.replace("'", "")
                )
                if "None" in empresa_ente_federativo_responsavel:
                    empresa_ente_federativo_responsavel = None
            resposta_cnpj[
                "empresa_ente_federativo_responsavel"
            ] = empresa_ente_federativo_responsavel

            # Realizar consulta complementar para buscar os dados de simples:
            empresa_cnpj = remove_breaks(str(campos_cnpj["empresa_cnpj"]))
            sql_simples = f"SELECT * from simples where cnpj_basico = '{empresa_cnpj}'"
            cursor.execute(sql_simples)
            results = cursor.fetchall()
            if results is not None and results != []:
                campos_simples = results[0]
                if campos_simples is None:
                    resposta_cnpj["simples_opcao_pelo_simples"] = None
                    resposta_cnpj["simples_data_opcao_pelo_simples"] = None
                    resposta_cnpj["simples_data_exclusao_pelo_simples"] = None
                    resposta_cnpj["simples_opcao_pelo_mei"] = None
                    resposta_cnpj["simples_data_opcao_pelo_mei"] = None
                    resposta_cnpj["simples_data_exclusao_pelo_mei"] = None
                else:
                    resposta_cnpj["simples_opcao_pelo_simples"] = campos_simples[
                        "opcao_pelo_simples"
                    ]
                    simples_data_opcao_pelo_simples = campos_simples[
                        "data_opcao_pelo_simples"
                    ]
                    if simples_data_opcao_pelo_simples is not None:
                        resposta_cnpj["simples_data_opcao_pelo_simples"] = formata_data(
                            simples_data_opcao_pelo_simples
                        )
                    else:
                        resposta_cnpj["simples_data_opcao_pelo_simples"] = None

                    simples_data_exclusao_pelo_simples = campos_simples[
                        "data_exclusao_pelo_simples"
                    ]
                    if simples_data_exclusao_pelo_simples:
                        resposta_cnpj[
                            "simples_data_exclusao_pelo_simples"
                        ] = formata_data(simples_data_exclusao_pelo_simples)
                    else:
                        resposta_cnpj["simples_data_exclusao_pelo_simples"] = None
                    resposta_cnpj["simples_opcao_pelo_mei"] = campos_simples[
                        "opcao_pelo_mei"
                    ]

                    simples_data_opcao_pelo_mei = campos_simples["data_opcao_pelo_mei"]
                    if simples_data_opcao_pelo_mei is not None:
                        resposta_cnpj["simples_data_opcao_pelo_mei"] = formata_data(
                            simples_data_opcao_pelo_mei
                        )
                    else:
                        resposta_cnpj["simples_data_opcao_pelo_mei"] = None

                    simples_data_exclusao_pelo_mei = campos_simples[
                        "data_exclusao_pelo_mei"
                    ]
                    if simples_data_exclusao_pelo_mei is not None:
                        resposta_cnpj["simples_data_exclusao_pelo_mei"] = formata_data(
                            simples_data_exclusao_pelo_mei
                        )
                    else:
                        resposta_cnpj["simples_data_exclusao_pelo_mei"] = None
            else:
                resposta_cnpj["simples_opcao_pelo_simples"] = None
                resposta_cnpj["simples_data_opcao_pelo_simples"] = None
                resposta_cnpj["simples_data_exclusao_pelo_simples"] = None
                resposta_cnpj["simples_opcao_pelo_mei"] = None
                resposta_cnpj["simples_data_opcao_pelo_mei"] = None
                resposta_cnpj["simples_data_exclusao_pelo_mei"] = None
        return resposta_cnpj
    else:
        logging.error("Cursor nulo!")
        return None
    pass


# Processar a partir de um CNPJ as tabelas relacionadas ao estabelecimento
def process_resposta_cnpjs_estabelecimento(
    cnpj_basico: str, cnpj_ordem: str, cnpj_dv: str, cursor=None
):

    # Executa uma consulta ao banco para retornar com join as informações das tabelas complementares e montar o registro a ser salvo na tabela resposta_cnpj
    sql = "SELECT estabelecimento.cnpj_basico as estabelecimento_cnpj_basico,"
    sql = sql + " estabelecimento.cnpj_ordem as estabelecimento_cnpj_ordem,"
    sql = sql + " estabelecimento.cnpj_dv as estabelecimento_cnpj_dv,"
    sql = (
        sql
        + " estabelecimento.identificador_matriz_filial as estabelecimento_identificador_matriz_filial,"
    )
    sql = sql + " estabelecimento.nome_fantasia as estabelecimento_nome_fantasia,"
    sql = (
        sql
        + " estabelecimento.situacao_cadastral as estabelecimento_situacao_cadastral,"
    )
    sql = (
        sql
        + " estabelecimento.data_situacao_cadastral as estabelecimento_data_situacao_cadastral,"
    )
    sql = (
        sql
        + " estabelecimento.motivo_situacao_cadastral as estabelecimento_motivo_situacao_cadastral,"
    )
    sql = (
        sql
        + " estabelecimento.nome_cidade_exterior as estabelecimento_nome_cidade_exterior,"
    )
    sql = sql + " estabelecimento.pais as estabelecimento_pais,"
    sql = (
        sql
        + " estabelecimento.data_inicio_atividade as estabelecimento_data_inicio_atividade,"
    )
    sql = sql + " estabelecimento.cnae_fiscal as estabelecimento_cnae_fiscal,"
    sql = (
        sql
        + " estabelecimento.cnae_fiscal_secundario as estabelecimento_cnae_fiscal_secundario,"
    )
    sql = sql + " estabelecimento.tipo_logradouro as estabelecimento_tipo_logradouro,"
    sql = sql + " estabelecimento.logradouro as estabelecimento_logradouro,"
    sql = sql + " estabelecimento.numero as estabelecimento_numero,"
    sql = sql + " estabelecimento.complemento as estabelecimento_complemento,"
    sql = sql + " estabelecimento.bairro as estabelecimento_bairro,"
    sql = sql + " estabelecimento.cep as estabelecimento_cep,"
    sql = sql + " estabelecimento.uf as estabelecimento_uf,"
    sql = sql + " estabelecimento.municipio as estabelecimento_municipio,"
    sql = sql + " estabelecimento.ddd_1 as estabelecimento_ddd_1,"
    sql = sql + " estabelecimento.telefone_1 as estabelecimento_telefone_1,"
    sql = sql + " estabelecimento.ddd_2 as estabelecimento_ddd_2,"
    sql = sql + " estabelecimento.telefone_2 as estabelecimento_telefone_2,"
    sql = sql + " estabelecimento.ddd_fax as estabelecimento_ddd_fax,"
    sql = sql + " estabelecimento.telefone_fax as estabelecimento_telefone_fax,"
    sql = (
        sql
        + " estabelecimento.correio_eletronico as estabelecimento_correio_eletronico,"
    )
    sql = (
        sql + " estabelecimento.situacao_especial as estabelecimento_situacao_especial,"
    )
    sql = (
        sql
        + " estabelecimento.data_situacao_especial as estabelecimento_data_situacao_especial"
    )
    sql = sql + " FROM estabelecimento"
    sql = (
        sql
        + f" WHERE estabelecimento.cnpj_basico='{cnpj_basico}' AND estabelecimento.cnpj_ordem ='{cnpj_ordem}' AND estabelecimento.cnpj_dv='{cnpj_dv}';"
    )

    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        # Armazena os campos a serem salvos na tabela de resposta_cnpj
        resposta_cnpj = {}
        # Objeto auxiliar recebe uma cópia dos campos do resultado da consulta executada no banco
        campos_cnpj = {}
        if results is not None and results != []:
            r = results[0]
            campos_cnpj = r.copy()
            # Sanitização e montagem dos campos compostos salvando num objeto único resposta_cnpj a ser submetido
            # Mantém o valor da tabela emresa como referência
            resposta_cnpj["estabelecimento_cnpj_basico"] = cnpj_basico

            # Corrige a remoção dos zeros à esquerda, sempre deve ter 4 dígitos.
            estabelecimento_cnpj_ordem = campos_cnpj["estabelecimento_cnpj_ordem"]
            if estabelecimento_cnpj_ordem is not None:
                while len(estabelecimento_cnpj_ordem) < 4:
                    logging.info(
                        "* Remoção de zeros à esquerda e garantia da corretude do campo ORDEM do CNPJ"
                    )
                    estabelecimento_cnpj_ordem = "0" + estabelecimento_cnpj_ordem
                resposta_cnpj["estabelecimento_cnpj_ordem"] = estabelecimento_cnpj_ordem
            else:
                resposta_cnpj["estabelecimento_cnpj_ordem"] = None

            resposta_cnpj["estabelecimento_cnpj_dv"] = campos_cnpj[
                "estabelecimento_cnpj_dv"
            ]

            # Busca informações da tabela dim_matriz_filial
            identificador_matriz_filial = str(
                campos_cnpj["estabelecimento_identificador_matriz_filial"]
            )

            if identificador_matriz_filial != "":
                # SQL a ser realizada para buscar as informações sobre matriz e filial
                sql = f"select * from dim_matriz_filial where codigo = '{identificador_matriz_filial}'"
                cursor.execute(sql)
                results = cursor.fetchall()
                if results is not None and results != []:
                    # Should be a single result, fetch first item
                    campos = results[0]
                    if campos is not None:
                        codigo = str(campos["codigo"])
                        descricao = str(campos["descricao"])
                        resposta_cnpj[
                            "estabelecimento_identificador_matriz_filial"
                        ] = f"{codigo} - {descricao}"
                    else:
                        resposta_cnpj[
                            "estabelecimento_identificador_matriz_filial"
                        ] = identificador_matriz_filial
                else:
                    logging.error(
                        f"Erro! Identificador matriz/filial {identificador_matriz_filial} não encontrado"
                    )
                    resposta_cnpj[
                        "estabelecimento_identificador_matriz_filial"
                    ] = identificador_matriz_filial
            else:
                resposta_cnpj["estabelecimento_identificador_matriz_filial"] = None

            # Nome fantasia do estabelecimento
            estabelecimento_nome_fantasia = str(
                campos_cnpj["estabelecimento_nome_fantasia"]
            )
            if estabelecimento_nome_fantasia is not None:
                estabelecimento_nome_fantasia = remove_breaks(
                    estabelecimento_nome_fantasia.replace("'", "`")
                )
                # Remove espaços em branco
                if "None" in estabelecimento_nome_fantasia:
                    estabelecimento_nome_fantasia = None
                resposta_cnpj[
                    "estabelecimento_nome_fantasia"
                ] = estabelecimento_nome_fantasia
            else:
                resposta_cnpj["estabelecimento_nome_fantasia"] = None

            # Busca informações sobre a dim_situacao_cadastral
            situacao_cadastral = str(campos_cnpj["estabelecimento_situacao_cadastral"])
            if situacao_cadastral != "":
                sql = f"select * from dim_situacao_cadastral where codigo = '{situacao_cadastral}'"
                cursor.execute(sql)
                results = cursor.fetchall()
                if results is not None and results != []:
                    # Should be a single result, fetch first item
                    campos = results[0]
                    if campos is not None:
                        codigo = str(campos["codigo"])
                        descricao = str(campos["descricao"])
                        resposta_cnpj[
                            "estabelecimento_situacao_cadastral"
                        ] = f"{codigo} - {descricao}"
                    else:
                        resposta_cnpj[
                            "estabelecimento_situacao_cadastral"
                        ] = situacao_cadastral
                else:
                    logging.error(
                        f"Erro! Situação cadastral {situacao_cadastral} não encontrada"
                    )
                    resposta_cnpj[
                        "estabelecimento_situacao_cadastral"
                    ] = situacao_cadastral
            else:
                resposta_cnpj["estabelecimento_situacao_cadastral"] = None

            # Data da situação cadastral do estabelecimento
            estabelecimento_data_situacao_cadastral = campos_cnpj[
                "estabelecimento_data_situacao_cadastral"
            ]
            if estabelecimento_data_situacao_cadastral is not None:
                if (
                    estabelecimento_data_situacao_cadastral != "0"
                    and estabelecimento_data_situacao_cadastral != ""
                ):
                    data_formatada = formata_data(
                        estabelecimento_data_situacao_cadastral
                    )
                    data_formatada = remove_breaks(data_formatada)
                    resposta_cnpj[
                        "estabelecimento_data_situacao_cadastral"
                    ] = data_formatada
                else:
                    resposta_cnpj["estabelecimento_data_situacao_cadastral"] = None
            else:
                resposta_cnpj["estabelecimento_data_situacao_cadastral"] = None

            # Busca informações sobre o motivo da situação cadastral
            motivo_situacao_cadastral = str(
                campos_cnpj["estabelecimento_motivo_situacao_cadastral"]
            )
            if motivo_situacao_cadastral != "":
                sql = (
                    f"select * from motivo where codigo = '{motivo_situacao_cadastral}'"
                )
                cursor.execute(sql)
                results = cursor.fetchall()
                if results is not None and results != []:
                    # Should be a single result, fetch first item
                    campos = results[0]
                    if campos is not None:
                        codigo = str(campos["codigo"])
                        descricao = str(campos["descricao"])
                        resposta_cnpj[
                            "estabelecimento_motivo_situacao_cadastral"
                        ] = f"{codigo} - {descricao}"
                    else:
                        resposta_cnpj[
                            "estabelecimento_motivo_situacao_cadastral"
                        ] = motivo_situacao_cadastral
                else:
                    logging.error(
                        f"Erro! Situação cadastral {motivo_situacao_cadastral} não encontrada"
                    )
                    resposta_cnpj[
                        "estabelecimento_motivo_situacao_cadastral"
                    ] = motivo_situacao_cadastral
            else:
                resposta_cnpj["estabelecimento_motivo_situacao_cadastral"] = None

            # Nome da cidade no exterior, caso se aplique
            estabelecimento_nome_cidade_exterior = campos_cnpj[
                "estabelecimento_nome_cidade_exterior"
            ]
            if estabelecimento_nome_cidade_exterior is not None:
                estabelecimento_nome_cidade_exterior = (
                    estabelecimento_nome_cidade_exterior.replace("'", "`")
                )
                estabelecimento_nome_cidade_exterior = remove_breaks(
                    estabelecimento_nome_cidade_exterior
                )
                if (
                    "None" in estabelecimento_nome_cidade_exterior
                    or estabelecimento_nome_cidade_exterior == ""
                ):
                    estabelecimento_nome_cidade_exterior = None
                resposta_cnpj[
                    "estabelecimento_nome_cidade_exterior"
                ] = estabelecimento_nome_cidade_exterior
            else:
                resposta_cnpj["estabelecimento_nome_cidade_exterior"] = None

            # Data de início da atividade empresarial
            estabelecimento_data_inicio_atividade = campos_cnpj[
                "estabelecimento_data_inicio_atividade"
            ]
            if estabelecimento_data_inicio_atividade is not None:
                data_formatada = formata_data(estabelecimento_data_inicio_atividade)
                resposta_cnpj["estabelecimento_data_inicio_atividade"] = data_formatada
            else:
                resposta_cnpj["estabelecimento_data_inicio_atividade"] = None

            # Realiza busca da lista de cnaes secundários na tabela cnae
            estabelecimento_cnae_fiscal_secundario = campos_cnpj[
                "estabelecimento_cnae_fiscal_secundario"
            ]
            if estabelecimento_cnae_fiscal_secundario is not None:
                if estabelecimento_cnae_fiscal_secundario != "":
                    items = estabelecimento_cnae_fiscal_secundario.split(",")
                    items_desc_completa = []
                    if len(items) >= 1:
                        for cnae_codigo in items:
                            # SQL a ser realizada para buscar as informações do país
                            sql_cnae = (
                                f"select * from cnae where codigo = '{cnae_codigo}'"
                            )
                            cursor.execute(sql_cnae)
                            results = cursor.fetchall()
                            if results is not None and results != []:
                                # SHould be a single result, fetch first
                                campos_cnae = results[0]
                                cnae_cod_desc = ""
                                if campos_cnae is not None:
                                    cnae_descricao = str(campos_cnae["descricao"])
                                    cnae_cod_desc = f"{cnae_codigo} - {cnae_descricao}"
                                else:
                                    cnae_cod_desc = cnae_codigo

                                # Adiciona à lista de objetos codificado cnae secundário
                                items_desc_completa.append(cnae_cod_desc)

                            else:
                                logging.error(
                                    f"Erro! CNAE {cnae_codigo} não encontrado"
                                )
                        resposta_cnpj[
                            "estabelecimento_cnae_fiscal_secundario"
                        ] = ";".join(items_desc_completa)
            else:
                resposta_cnpj["estabelecimento_cnae_fiscal_secundario"] = None

            # Descrição do tipo de logradouro
            resposta_cnpj["estabelecimento_tipo_logradouro"] = campos_cnpj[
                "estabelecimento_tipo_logradouro"
            ]

            # Descrição do logradouro
            resposta_cnpj["estabelecimento_logradouro"] = remove_breaks(
                str(campos_cnpj["estabelecimento_logradouro"]).replace("'", "`")
            )

            # Definição do número do logradouro
            resposta_cnpj["estabelecimento_numero"] = remove_breaks(
                str(campos_cnpj["estabelecimento_numero"]).replace("'", "")
            )

            # Descrição textual complementar do endereço
            estabelecimento_complemento = str(
                campos_cnpj["estabelecimento_complemento"]
            )
            if estabelecimento_complemento is not None:
                estabelecimento_complemento = remove_breaks(
                    estabelecimento_complemento.replace("'", "`")
                )
                if (
                    "None" in estabelecimento_complemento
                    or estabelecimento_complemento == ""
                ):
                    estabelecimento_complemento = None
                resposta_cnpj[
                    "estabelecimento_complemento"
                ] = estabelecimento_complemento
            else:
                resposta_cnpj["estabelecimento_complemento"] = None

            # Bairro do logradouro
            resposta_cnpj["estabelecimento_bairro"] = remove_breaks(
                str(campos_cnpj["estabelecimento_bairro"]).replace("'", "`")
            )

            # CEP do logradouro
            cep = campos_cnpj["estabelecimento_cep"]
            if cep is not None:
                # Remove o ponto
                cep = remove_breaks(cep.replace(".", ""))
                # Corrige erros de corte de zero à esquerda
                while len(cep) < 8:
                    cep = "0" + cep
                # Formata CEP
                resposta_cnpj["estabelecimento_cep"] = cep
            else:
                resposta_cnpj["estabelecimento_cep"] = None

            resposta_cnpj["estabelecimento_uf"] = remove_breaks(
                campos_cnpj["estabelecimento_uf"]
            )

            # Telefone 1
            estabelecimento_ddd_1 = campos_cnpj["estabelecimento_ddd_1"]
            estabelecimento_telefone_1 = campos_cnpj["estabelecimento_telefone_1"]
            if estabelecimento_ddd_1 and estabelecimento_telefone_1:
                estabelecimento_ddd_telefone_1 = (
                    estabelecimento_ddd_1 + estabelecimento_telefone_1
                )
                estabelecimento_ddd_telefone_1 = remove_breaks(
                    estabelecimento_ddd_telefone_1
                )
            else:
                estabelecimento_ddd_telefone_1 = None
            resposta_cnpj[
                "estabelecimento_ddd_telefone_1"
            ] = estabelecimento_ddd_telefone_1

            # Telefone 2
            estabelecimento_ddd_2 = campos_cnpj["estabelecimento_ddd_2"]
            estabelecimento_telefone_2 = campos_cnpj["estabelecimento_telefone_2"]
            if estabelecimento_ddd_2 and estabelecimento_telefone_2:
                estabelecimento_ddd_telefone_2 = (
                    estabelecimento_ddd_2 + estabelecimento_telefone_2
                )
                estabelecimento_ddd_telefone_2 = remove_breaks(
                    estabelecimento_ddd_telefone_2
                )
            else:
                estabelecimento_ddd_telefone_2 = None
            resposta_cnpj[
                "estabelecimento_ddd_telefone_2"
            ] = estabelecimento_ddd_telefone_2

            # Fax
            estabelecimento_ddd_fax = campos_cnpj["estabelecimento_ddd_fax"]
            estabelecimento_telefone_fax = campos_cnpj["estabelecimento_telefone_fax"]
            if estabelecimento_ddd_fax and estabelecimento_telefone_fax:
                estabelecimento_ddd_telefone_fax = (
                    estabelecimento_ddd_fax
                ) = estabelecimento_telefone_fax
                estabelecimento_ddd_telefone_fax = remove_breaks(
                    estabelecimento_ddd_telefone_fax
                )
            else:
                estabelecimento_ddd_telefone_fax = None
            resposta_cnpj[
                "estabelecimento_ddd_telefone_fax"
            ] = estabelecimento_ddd_telefone_fax

            # Correio Eletrônico
            estabelecimento_correio_eletronico = str(
                campos_cnpj["estabelecimento_correio_eletronico"]
            )
            if estabelecimento_correio_eletronico:
                estabelecimento_correio_eletronico = remove_breaks(
                    estabelecimento_correio_eletronico
                )
                estabelecimento_correio_eletronico = (
                    estabelecimento_correio_eletronico.replace('"', "")
                )
                estabelecimento_correio_eletronico = (
                    estabelecimento_correio_eletronico.replace("'", "")
                )
                resposta_cnpj[
                    "estabelecimento_correio_eletronico"
                ] = estabelecimento_correio_eletronico
            else:
                resposta_cnpj["estabelecimento_correio_eletronico"] = None

            # Situação especial
            estabelecimento_situacao_especial = campos_cnpj[
                "estabelecimento_situacao_especial"
            ]
            if estabelecimento_situacao_especial is not None:
                estabelecimento_situacao_especial = remove_breaks(
                    estabelecimento_situacao_especial
                )
                if (
                    "None" in estabelecimento_situacao_especial
                    or estabelecimento_situacao_especial == ""
                ):
                    estabelecimento_situacao_especial = None
            resposta_cnpj[
                "estabelecimento_situacao_especial"
            ] = estabelecimento_situacao_especial

            estabelecimento_data_situacao_especial = campos_cnpj[
                "estabelecimento_data_situacao_especial"
            ]
            if estabelecimento_data_situacao_especial is not None:
                if (
                    "None" in estabelecimento_data_situacao_especial
                    or estabelecimento_data_situacao_especial == ""
                ):
                    estabelecimento_data_situacao_especial = None
            resposta_cnpj[
                "estabelecimento_data_situacao_especial"
            ] = estabelecimento_data_situacao_especial

            # Consulta complementar para dados do CNAE
            cnae_codigo = remove_breaks(str(campos_cnpj["estabelecimento_cnae_fiscal"]))
            if cnae_codigo != "":
                # SQL a ser realizada para buscar as informações do país
                sql_cnae = f"select * from cnae where codigo = '{cnae_codigo}'"
                cursor.execute(sql_cnae)
                results = cursor.fetchall()
                if results is not None and results != []:
                    # SHould be a single result, fetch first
                    campos_cnae = results[0]
                    cnae_cod_desc = ""
                    if campos_cnae is not None:
                        cnae_descricao = str(campos_cnae["descricao"])
                        cnae_cod_desc = f"{cnae_codigo} - {cnae_descricao}"
                    else:
                        cnae_cod_desc = cnae_codigo
                    resposta_cnpj["cnae"] = cnae_cod_desc
                else:
                    logging.error(f"Erro! CNAE {cnae_codigo} não encontrado")
                    resposta_cnpj["cnae"] = str(
                        campos_cnpj["estabelecimento_cnae_fiscal"]
                    )
            else:
                resposta_cnpj["cnae"] = None

            # Consulta à tabela País.
            # Caso o código do país não seja None, evitando cancelamento da query em INNER JOIN com pais com codigo None
            pais_codigo = campos_cnpj["estabelecimento_pais"]
            # Brasil está vindo como vazio, então será explicitamente adicionado o código 105
            if pais_codigo is None:
                pais_codigo = "105"

            pais_codigo = remove_breaks(pais_codigo)
            pais_codigo = pais_codigo.strip()

            # Talvez não seja mais necessário
            if (pais_codigo == "") or ("None" in pais_codigo):
                pais_codigo = "105"

            if ".0" in pais_codigo:
                # Remove '.0'
                pais_codigo = pais_codigo[:-2]
            # Completa com zeros à esquerda até o número ficar com 3 dígitos
            while len(pais_codigo) < 3:
                pais_codigo = "0" + pais_codigo

            # SQL a ser realizada para buscar as informações do país
            sql_pais = f"select * from pais where codigo = '{pais_codigo}'"
            cursor.execute(sql_pais)
            results = cursor.fetchall()
            if results is not None and results != []:
                # Should be a single result, fetch first item
                campos_pais = results[0]
                if campos_pais is not None:
                    pais_descricao = str(campos_pais["descricao"])
                    pais_cod_desc = f"{pais_codigo} - {pais_descricao}"
                else:
                    pais_cod_desc = pais_cod
                resposta_cnpj["pais"] = f"{pais_cod_desc}"
            else:
                logging.error(f"Erro! País {pais_codigo} não encontrado")
                resposta_cnpj["pais"] = str(campos_cnpj["estabelecimento_pais"])

            # Consulta para busca do município:
            municipio_codigo = str(campos_cnpj["estabelecimento_municipio"])
            if municipio_codigo != "" and "None" not in municipio_codigo:
                sql_municipio = (
                    f"select * from municipio where codigo = '{municipio_codigo}'"
                )
                if cursor is not None:
                    cursor.execute(sql_municipio)
                    results = cursor.fetchall()
                    if results is not None and results != []:
                        # Should be a single result, fetch first
                        campos_municipio = results[0]
                        if campos_municipio is not None:
                            municipio_descricao = str(
                                campos_municipio["descricao"]
                            ).replace("'", "`")
                            municipio_cod_desc = (
                                f"{municipio_codigo} - {municipio_descricao}"
                            )
                        else:
                            municipio_cod_desc = municipio_cod
                        resposta_cnpj["municipio"] = f"{municipio_cod_desc}"
                    else:
                        logging.error(
                            f"Erro! Município {municipio_codigo} não encontrado."
                        )
                        resposta_cnpj["municipio"] = str(
                            campos_cnpj["estabelecimento_municipio"]
                        )
            else:
                resposta_cnpj["municipio"] = None
            return resposta_cnpj
    else:
        logging.info("Cursor is None")
        return None
    pass


# Método auxiliar utilizado para completar com dados em branco os campos relacionados ao dicionários da tabela estabelecimento
def set_estabelecimento_blank(resposta_cnpj):
    if resposta_cnpj is None:
        resposta_cnpj = {}
    resposta_cnpj["estabelecimento_cnpj_ordem"] = None
    resposta_cnpj["estabelecimento_cnpj_dv"] = None
    resposta_cnpj["estabelecimento_identificador_matriz_filial"] = None
    resposta_cnpj["estabelecimento_nome_fantasia"] = None
    resposta_cnpj["estabelecimento_situacao_cadastral"] = None
    resposta_cnpj["estabelecimento_data_situacao_cadastral"] = None
    resposta_cnpj["estabelecimento_motivo_situacao_cadastral"] = None
    resposta_cnpj["estabelecimento_nome_cidade_exterior"] = None
    resposta_cnpj["estabelecimento_data_inicio_atividade"] = None
    resposta_cnpj["estabelecimento_cnae_fiscal_secundario"] = None
    resposta_cnpj["estabelecimento_tipo_logradouro"] = None
    resposta_cnpj["estabelecimento_logradouro"] = None
    resposta_cnpj["estabelecimento_numero"] = None
    resposta_cnpj["estabelecimento_complemento"] = None
    resposta_cnpj["estabelecimento_bairro"] = None
    resposta_cnpj["estabelecimento_cep"] = None
    resposta_cnpj["estabelecimento_uf"] = None
    resposta_cnpj["estabelecimento_ddd_telefone_1"] = None
    resposta_cnpj["estabelecimento_ddd_telefone_2"] = None
    resposta_cnpj["estabelecimento_ddd_telefone_fax"] = None
    resposta_cnpj["estabelecimento_correio_eletronico"] = None
    resposta_cnpj["estabelecimento_situacao_especial"] = None
    resposta_cnpj["estabelecimento_data_situacao_especial"] = None
    resposta_cnpj["cnae"] = None
    resposta_cnpj["pais"] = None
    resposta_cnpj["municipio"] = None
    return resposta_cnpj


# Processar a partir de um CNPJ as tabelas relacionadas aos sócios da empresa
def process_resposta_socios(cnpj_basico: str, cursor=None):
    logging.info(f"Processando socio para CNPJ {cnpj_basico}")
    # Executa uma consulta ao banco para retornar com join as informações das tabelas complementares e montar o registro a ser salvo na tabela resposta_cnpj
    sql = "SELECT socio.cnpj_basico as socio_cnpj_basico,"
    sql = sql + " socio.identificador_socio as socio_identificador_socio,"
    sql = sql + " socio.razao_social as socio_razao_social,"
    sql = sql + " socio.cnpj_cpf_socio as socio_cnpj_cpf_socio,"
    sql = sql + " socio.codigo_qualificacao_socio as socio_codigo_qualificacao_socio,"
    sql = sql + " socio.data_entrada_sociedade as socio_data_entrada_sociedade,"
    sql = (
        sql
        + " socio.codigo_pais_socio_estrangeiro as socio_codigo_pais_socio_estrangeiro,"
    )
    sql = (
        sql
        + " socio.numero_cpf_representante_legal as socio_numero_cpf_representante_legal,"
    )
    sql = sql + " socio.nome_representante_legal as socio_nome_representante_legal,"
    sql = (
        sql
        + " socio.codigo_qualificacao_representante_legal as socio_codigo_qualificacao_representante_legal,"
    )
    sql = sql + " socio.faixa_etaria as socio_faixa_etaria"
    sql = sql + " FROM socio"
    sql = sql + " INNER JOIN empresa ON socio.cnpj_basico = empresa.cnpj"
    sql = sql + f" WHERE empresa.cnpj =  '{cnpj_basico}';"
    if cursor is not None:
        results = None
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except Exception as e:
            logging.error(e)
        # Armazena os campos a serem salvos na tabela de resposta_socios
        resposta_socios = {}
        campos_socios = {}
        lista_resposta_socios = []

        if results is not None and results != []:
            for r in results:
                campos_socios = r.copy()
                # Sanitização e montagem dos campos compostos salvando uma linha para cada sócio do CNPJ a ser submetida na tabela resposta_socios
                resposta_socios["socio_cnpj_basico"] = campos_socios[
                    "socio_cnpj_basico"
                ]
                # Realizar a consulta complementar para dim_identificador_socio
                identificador_socio = str(campos_socios["socio_identificador_socio"])
                if identificador_socio != "":
                    # SQL a ser realizada para buscar as informações
                    sql = f"select * from dim_identificador_socio where codigo = '{identificador_socio}'"
                    results = None
                    try:
                        cursor.execute(sql)
                        results = cursor.fetchall()
                    except Exception as e:
                        logging.error(e)
                    if results is not None and results != []:
                        # SHould be a single result, fetch first
                        campos = results[0]
                        cod_desc = ""
                        if campos is not None:
                            codigo = str(campos["codigo"])
                            descricao = str(campos["descricao"])
                            resposta_socios[
                                "socio_identificador_socio"
                            ] = f"{codigo} - {descricao}"
                        else:
                            resposta_socios[
                                "socio_identificador_socio"
                            ] = identificador_socio
                    else:
                        resposta_socios[
                            "socio_identificador_socio"
                        ] = identificador_socio
                        logging.error(
                            f"Erro! Identificador Sócio {identificador_socio} não encontrado"
                        )
                else:
                    resposta_socios["socio_identificador_socio"] = None

                resposta_socios["socio_razao_social"] = campos_socios[
                    "socio_razao_social"
                ]
                resposta_socios["socio_cnpj_cpf_socio"] = campos_socios[
                    "socio_cnpj_cpf_socio"
                ]

                # Run select to try to fetch data
                socio_codigo_qualificacao_socio = campos_socios[
                    "socio_codigo_qualificacao_socio"
                ]
                if socio_codigo_qualificacao_socio is not None:
                    sql_qualificacao_socio = f"SELECT * from qualificacao_socio where codigo = '{socio_codigo_qualificacao_socio}'"
                    results_qs = None
                    try:
                        cursor.execute(sql_qualificacao_socio)
                        results_qs = cursor.fetchall()
                    except Exception as e:
                        logging.error(e)

                    if results_qs is not None and results_qs != []:
                        r = results_qs[0]
                        if r is not None:
                            resposta_socios["socio_codigo_qualificacao_socio"] = (
                                socio_codigo_qualificacao_socio + " - " + r["descricao"]
                            )
                        else:
                            resposta_socios[
                                "socio_codigo_qualificacao_socio"
                            ] = socio_codigo_qualificacao_socio
                else:
                    resposta_socios["socio_codigo_qualificacao_socio"] = None

                # Formatar data de entrada na sociedade
                socio_data_entrada_sociedade = campos_socios[
                    "socio_data_entrada_sociedade"
                ]
                if socio_data_entrada_sociedade is not None:
                    data_formatada = formata_data(socio_data_entrada_sociedade)
                resposta_socios["socio_data_entrada_sociedade"] = data_formatada

                # Realizar consulta à tabela país, caso o código do país não seja None, evitando cancelamento da query em INNER JOIN com pais com codigo None
                pais_codigo = str(campos_socios["socio_codigo_pais_socio_estrangeiro"])
                if pais_codigo != "":
                    # Brasil
                    if "None" in pais_codigo:
                        pais_codigo = "105"
                    # Remove '.0'
                    if ".0" in pais_codigo:
                        pais_codigo = pais_codigo[:-2]
                    # Preenche com zeros à esquerda:
                    while len(pais_codigo) < 3:
                        pais_codigo = "0" + pais_codigo

                    # SQL a ser realizada para buscar as informações do país
                    sql_pais = f"select * from pais where codigo = '{pais_codigo}'"
                    results = None
                    try:
                        cursor.execute(sql_pais)
                        results = cursor.fetchall()
                    except Exception as e:
                        logging.error(e)
                    if results is not None and results != []:
                        # Should be a single result, fetch first
                        campos_pais = results[0]
                        if campos_pais is not None:
                            pais_descricao = str(campos_pais["descricao"])
                            pais_cod_desc = f"{pais_codigo} - {pais_descricao}"
                        else:
                            pais_cod_desc = pais_cod
                        resposta_socios["pais_socio_estrangeiro"] = f"{pais_cod_desc}"
                    else:
                        logging.error(f"Erro! País {pais_codigo} não encontrado")
                        resposta_socios["pais_socio_estrangeiro"] = pais_codigo
                else:
                    resposta_socios["pais_socio_estrangeiro"] = str(
                        campos_socios["socio_codigo_pais_socio_estrangeiro"]
                    )

                resposta_socios["socio_numero_cpf_representante_legal"] = campos_socios[
                    "socio_numero_cpf_representante_legal"
                ]
                resposta_socios["socio_nome_representante_legal"] = campos_socios[
                    "socio_nome_representante_legal"
                ]
                resposta_socios[
                    "socio_codigo_qualificacao_representante_legal"
                ] = campos_socios["socio_codigo_qualificacao_representante_legal"]

                # Select from dim_faixa_etaria
                faixa_etaria = str(campos_socios["socio_faixa_etaria"])
                if faixa_etaria != "":
                    # SQL a ser realizada para buscar as informações do país
                    sql = f"select * from dim_faixa_etaria where codigo = '{faixa_etaria}'"
                    results = None
                    try:
                        cursor.execute(sql)
                        results = cursor.fetchall()
                    except Exception as e:
                        logging.error(e)
                    if results is not None and results != []:
                        # Should be a single result, fetch first
                        campos = results[0]
                        cod_desc = ""
                        if campos is not None:
                            codigo = str(campos["codigo"])
                            descricao = str(campos["descricao"])
                            resposta_socios[
                                "socio_faixa_etaria"
                            ] = f"{codigo} - {descricao}"
                        else:
                            resposta_socios["socio_faixa_etaria"] = faixa_etaria
                    else:
                        resposta_socios["socio_faixa_etaria"] = identificador_socio
                        logging.error(
                            f"Erro! Faixa etária {identificador_socio} não encontrada"
                        )
                else:
                    resposta_socios["socio_faixa_etaria"] = None

                lista_resposta_socios.append(list(resposta_socios.values()))
            # Sai do laço for e executa uma única operação para salvar os dados no banco
            batch_insert_resposta_socios(cursor, lista_resposta_socios)
    pass


def conecta(password):
    try:
        conn = psycopg2.connect(
            database="qd_receita",
            user="postgres",
            password=password,
            host="localhost",
            port="5432",
            cursor_factory=RealDictCursor,
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        logging.error(e)


# Formata strings de data do tipo yyyymmdd para o padrão brasileiro dd/mm/yyyy
def formata_data(date):
    if date is not None:
        data_formatada = date[6:] + "/" + date[4:6] + "/" + date[0:4]
        return data_formatada
    else:
        return None


def generate_cnpj_dv(cnpj_without_dv: str) -> str:
    first_digit = calculate_cnpj_dv_digit(cnpj_without_dv)
    second_digit = calculate_cnpj_dv_digit(cnpj_without_dv, first_digit=first_digit)
    return first_digit + second_digit


def calculate_cnpj_dv_digit(cnpj_without_dv: str, first_digit: str = "") -> str:
    weights = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
    if first_digit == "":
        weights = weights[1:]

    cnpj_to_weight = cnpj_without_dv + first_digit
    weighted_digits = [
        int(num) * weight for num, weight in zip(cnpj_to_weight, weights)
    ]
    sum_remainder = sum(weighted_digits) % 11

    if sum_remainder < 2:
        return "0"
    else:
        return str(11 - sum_remainder)


if __name__ == "__main__":
    # Configurações de logging
    logging.basicConfig(
        handlers=[
            logging.FileHandler(
                filename="./process_cnpjs.log", encoding="utf-8", mode="a+"
            )
        ],
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        datefmt="%F %A %T",
        level=logging.INFO,
    )
    # Estabelece conexão com o BD
    conn = conecta("change HERE")
    if conn is not None:
        cursor = conn.cursor()
        offset = 0
        block_size = 1_000
        # Dicionário que vai receber a lista com todos os cnpjs a carregar, com uma chave por cnpj_basico e uma tupla (ordem,dv) como valor
        lista_cnpj_processados = []
        # Inicializa a variável
        dict_lista_cnpj_fatia = {}
        while dict_lista_cnpj_fatia is not None:
            try:
                # Pega uma lista de cnpjs da fatia
                dict_lista_cnpj_fatia = get_all_cnpj_ids(cursor, offset, block_size)
                # Se ainda há registros a processar
                if dict_lista_cnpj_fatia:
                    # Lista de objetos do tipo dicionário a serem inseridos em batch para a resposta_cnpj
                    lista_resposta_cnpj = []
                    # Resposta da empresa, processada apenas na filial
                    resposta_cnpjs_empresa = None
                    # Número de chaves cnpj_basico (empresas) a processar
                    n_chaves = len(dict_lista_cnpj_fatia.keys())
                    if n_chaves > 0:
                        # Para cada cnpj, espera-se que sejam retornados N estabelecimentos, sempre com apenas 1 empresa cada. Para cada estabelecimento, deve ser agregada a informação de ambos e persistida na tabela resposta_cnpj
                        chaves = dict_lista_cnpj_fatia.keys()
                        for cnpj_basico in chaves:
                            print(f"→→→ Processando cnpj_basico: {cnpj_basico}")
                            # Dicionário com os campos a serem salvos e consolidados na tabela resposta_cnpj
                            resposta_cnpj = None
                            # logging.info(f"* Processando dados para o CNPJ {cnpj_basico}")

                            # Primeira vez executando para este cnpj_basico, processa a resposta_empresa, resposta_socios e adiciona a lista de cnpj_carregados

                            # Empresa relacionada ao CNPJ, basta executar uma vez por CNPJ BASICO
                            resposta_cnpjs_empresa = process_resposta_cnpjs_empresa(
                                cnpj_basico, cursor
                            )
                            # Processa lista de sócios para o dado cnpj
                            process_resposta_socios(cnpj_basico, cursor)
                            print(f"→→→ Finalizou processamento SOCIOS {cnpj_basico}")
                            # Objetos estabelecimento relacionados a este CNPJ:
                            tuplas = dict_lista_cnpj_fatia[cnpj_basico]
                            if tuplas:
                                for tupla in tuplas:
                                    ordem = tupla[0]
                                    dv = tupla[1]
                                    if ordem and dv:
                                        resposta_cnpjs_estabelecimento = (
                                            process_resposta_cnpjs_estabelecimento(
                                                cnpj_basico,
                                                ordem,
                                                dv,
                                                cursor,
                                            )
                                        )
                                        # Caso hajam dados de empresa e estabelecimento, unifica o documento
                                        if (
                                            resposta_cnpjs_empresa
                                            and resposta_cnpjs_estabelecimento
                                        ):
                                            # Aqui é unificada em um único objeto resposta_cnpj a informação do estabelecimento com a informação da empresa
                                            resposta_cnpj = {
                                                **resposta_cnpjs_empresa,
                                                **resposta_cnpjs_estabelecimento,
                                            }
                                            # Adiciona o dicionário à lista de objetos a serem inseridos em batch
                                            lista_resposta_cnpj.append(
                                                list(resposta_cnpj.values())
                                            )
                        # Persiste os registros da fatia em batch no banco
                        batch_insert_resposta_cnpj(cursor, lista_resposta_cnpj)
                    # TODO: Verificar este fluxo de exceção
                    else:
                        lista_cnpj = None
                    # Encerra o laço, pega uma nova fatia a partir do incremento:
                    offset = offset + block_size
            except Exception as e:
                logging.error(f"Final da lista de CNPJs a carregar atingido.")
                logging.error(e)
            lista_cnpj_processados.append(dict_lista_cnpj_fatia)

            # logging.info("CNPJs carregados:")
            # logging.info(lista_cnpj_processados)
        # Final do laço while
        # Encerra a conexão com o BD
        conn.commit()
        conn.close()
