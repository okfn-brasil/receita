import psycopg2
from psycopg2.extras import RealDictCursor

def get_n_total_cnpj(cursor=None):
    # Executa uma consulta para pegar a quantidade total de CNPJs a processar
    sql = 'SELECT count(cnpj) as count_cnpj from empresa;'
    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        if results is not None:
            return results[0]['count_cnpj']

# Retorna um objeto com a resposta do banco
def get_all_cnpj_ids(cursor=None, offset=0, limit=1000):
    # Executa uma consulta para pegar todos os identificadores únicos de CNPJ que serão utilizados nas buscas:
    sql = f'SELECT empresa.cnpj as empresa_cnpj from empresa order by cnpj limit {limit} offset {offset}'
    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        if results is not None:
            return results

# Processar a partir de um CNPJ as tabelas relacionadas ao estabelecimento e seus socios
def process_resposta_cnpjs(cnpj_basico: str, cursor=None):
    # Executa uma consulta ao banco para retornar com join as informações das tabelas complementares e montar o registro a ser salvo na tabela resposta_cnpj
    sql = 'SELECT empresa.cnpj as empresa_cnpj,'
    sql = sql + ' empresa.razao_social as empresa_razao_social,'
    sql = sql + ' empresa.codigo_natureza_juridica as empresa_codigo_natureza_juridica,'
    sql = sql + ' empresa.qualificacao_do_responsavel as empresa_qualificacao_do_responsavel,'
    sql = sql + ' empresa.capital_social  as empresa_capital_social,'
    sql = sql + ' empresa.porte as empresa_porte,'
    sql = sql + ' empresa.ente_federativo_responsavel as empresa_ente_federativo_responsavel,'
    sql = sql + ' estabelecimento.cnpj_basico as estabelecimento_cnpj_basico,'
    sql = sql + ' estabelecimento.cnpj_ordem as estabelecimento_cnpj_ordem,'
    sql = sql + ' estabelecimento.cnpj_dv as estabelecimento_cnpj_dv,'
    sql = sql + ' estabelecimento.identificador_matriz_filial as estabelecimento_identificador_matriz_filial,'
    sql = sql + ' estabelecimento.nome_fantasia as estabelecimento_nome_fantasia,'
    sql = sql + ' estabelecimento.situacao_cadastral as estabelecimento_situacao_cadastral,'
    sql = sql + ' estabelecimento.data_situacao_cadastral as estabelecimento_data_situacao_cadastral,'
    sql = sql + ' estabelecimento.motivo_situacao_cadastral as estabelecimento_motivo_situacao_cadastral,'
    sql = sql + ' estabelecimento.nome_cidade_exterior as estabelecimento_nome_cidade_exterior,'
    sql = sql + ' estabelecimento.pais as estabelecimento_pais,'
    sql = sql + ' estabelecimento.data_inicio_atividade as estabelecimento_data_inicio_atividade,'
    sql = sql + ' estabelecimento.cnae_fiscal as estabelecimento_cnae_fiscal,'
    sql = sql + ' estabelecimento.cnae_fiscal_secundario as estabelecimento_cnae_fiscal_secundario,'
    sql = sql + ' estabelecimento.tipo_logradouro as estabelecimento_tipo_logradouro,'
    sql = sql + ' estabelecimento.logradouro as estabelecimento_logradouro,'
    sql = sql + ' estabelecimento.numero as estabelecimento_numero,'
    sql = sql + ' estabelecimento.complemento as estabelecimento_complemento,'
    sql = sql + ' estabelecimento.bairro as estabelecimento_bairro,'
    sql = sql + ' estabelecimento.cep as estabelecimento_cep,'
    sql = sql + ' estabelecimento.uf as estabelecimento_uf,'
    sql = sql + ' estabelecimento.municipio as estabelecimento_municipio,'
    sql = sql + ' estabelecimento.ddd_1 as estabelecimento_ddd_1,'
    sql = sql + ' estabelecimento.telefone_1 as estabelecimento_telefone_1,'
    sql = sql + ' estabelecimento.ddd_2 as estabelecimento_ddd_2,'
    sql = sql + ' estabelecimento.telefone_2 as estabelecimento_telefone_2,'
    sql = sql + ' estabelecimento.ddd_fax as estabelecimento_ddd_fax,'
    sql = sql + ' estabelecimento.telefone_fax as estabelecimento_telefone_fax,'
    sql = sql + ' estabelecimento.correio_eletronico as estabelecimento_correio_eletronico,'
    sql = sql + ' estabelecimento.situacao_especial as estabelecimento_situacao_especial,'
    sql = sql + ' estabelecimento.data_situacao_especial as estabelecimento_data_situacao_especial'
    sql = sql + ' FROM empresa'
    sql = sql + ' INNER JOIN estabelecimento ON empresa.cnpj = estabelecimento.cnpj_basico '
    sql = sql + f' WHERE empresa.cnpj =  \'{cnpj_basico}\';'

    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        # Armazena os campos a serem salvos na tabela de resposta_cnpj
        campos_cnpj = {}
        campos_simples = None
        campos_pais = None
        campos_municipio = None
        if results is not None:
            print(f'A busca retornou {len(results)} resultados:')
            for r in results:
                campos_cnpj = r.copy()
                sql_insert = 'INSERT into resposta_cnpj (estabelecimento_cnpj_basico, estabelecimento_cnpj_ordem, estabelecimento_cnpj_dv, estabelecimento_identificador_matriz_filial, estabelecimento_nome_fantasia, estabelecimento_situacao_cadastral, estabelecimento_data_situacao_cadastral, estabelecimento_motivo_situacao_cadastral, estabelecimento_nome_cidade_exterior, estabelecimento_data_inicio_atividade, estabelecimento_cnae_fiscal_secundario, estabelecimento_tipo_logradouro, estabelecimento_logradouro, estabelecimento_numero, estabelecimento_complemento, estabelecimento_bairro, estabelecimento_cep, estabelecimento_uf, estabelecimento_ddd_telefone_1, estabelecimento_ddd_telefone_2, estabelecimento_ddd_telefone_fax, estabelecimento_correio_eletronico, estabelecimento_situacao_especial, estabelecimento_data_situacao_especial, empresa_razao_social, empresa_codigo_natureza_juridica, empresa_qualificacao_do_responsavel, empresa_capital_social, empresa_porte, empresa_ente_federativo_responsavel, simples_opcao_pelo_simples, simples_data_opcao_pelo_simples, simples_data_exclusao_pelo_simples, simples_opcao_pelo_mei, simples_data_opcao_pelo_mei, simples_data_exclusao_pelo_mei, cnae, pais, municipio)'
                sql_insert = sql_insert + ' VALUES ('
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnpj_basico']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnpj_ordem']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnpj_dv']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_identificador_matriz_filial']) + '\', '
                estabelecimento_nome_fantasia = str(campos_cnpj['estabelecimento_nome_fantasia']).replace('\'', '`')
                sql_insert = sql_insert + '\'' + estabelecimento_nome_fantasia + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_situacao_cadastral']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_data_situacao_cadastral']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_motivo_situacao_cadastral']) + '\', '
                estabelecimento_nome_cidade_exterior = str(campos_cnpj['estabelecimento_nome_cidade_exterior']).replace('\'', '`')
                sql_insert = sql_insert + '\'' + estabelecimento_nome_cidade_exterior + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_data_inicio_atividade']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnae_fiscal_secundario']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_tipo_logradouro']) + '\', '
                # Evitando ocorrências de  aspas no texto dos logradouros
                estabelecimento_logradouro = str(campos_cnpj['estabelecimento_logradouro']).replace('\'', '`')
                sql_insert = sql_insert + '\'' + estabelecimento_logradouro + '\', '
                estabelecimento_numero = str(campos_cnpj['estabelecimento_numero']).replace('\'', '')
                sql_insert = sql_insert + '\'' + estabelecimento_numero + '\', '
                estabelecimento_complemento = str(campos_cnpj['estabelecimento_complemento']).replace('\'', '`')
                sql_insert = sql_insert + '\'' + estabelecimento_complemento + '\', '
                estabelecimento_bairro = str(campos_cnpj['estabelecimento_bairro']).replace('\'', '`')
                sql_insert = sql_insert + '\'' + estabelecimento_bairro + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cep']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_uf']) + '\', '
                # Tratando campos de telefone
                estabelecimento_ddd_telefone_1 = str(campos_cnpj['estabelecimento_ddd_1'])[:-2] + str(campos_cnpj['estabelecimento_telefone_1'])
                if 'None' in estabelecimento_ddd_telefone_1:
                    sql_insert = sql_insert + '\'\', '
                else:
                    sql_insert = sql_insert + '\'' + estabelecimento_ddd_telefone_1 + '\', '
                estabelecimento_ddd_telefone_2 = str(campos_cnpj['estabelecimento_ddd_2'])[:-2] + str(campos_cnpj['estabelecimento_telefone_2'])
                if 'None' in estabelecimento_ddd_telefone_2:
                    sql_insert = sql_insert + '\'\', '
                else:
                    sql_insert = sql_insert + '\'' + estabelecimento_ddd_telefone_2 + '\', '
                estabelecimento_ddd_telefone_fax = str(campos_cnpj['estabelecimento_ddd_fax'])[:-2] + str(campos_cnpj['estabelecimento_telefone_fax'])
                if 'None' in estabelecimento_ddd_telefone_fax:
                    sql_insert = sql_insert + '\'\', '
                else:
                    sql_insert = sql_insert + '\'' + estabelecimento_ddd_telefone_fax + '\', '
                estabelecimento_correio_eletronico = str(campos_cnpj['estabelecimento_correio_eletronico'])
                estabelecimento_correio_eletronico = estabelecimento_correio_eletronico.replace('\"', '')
                estabelecimento_correio_eletronico = estabelecimento_correio_eletronico.replace('\'', '')
                sql_insert = sql_insert + '\'' + estabelecimento_correio_eletronico + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_situacao_especial']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_data_situacao_especial']) + '\', '
                empresa_razao_social =  str(campos_cnpj['empresa_razao_social']).replace('\'', '`')
                sql_insert = sql_insert + '\'' + empresa_razao_social + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_codigo_natureza_juridica']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_qualificacao_do_responsavel']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_capital_social']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_porte']) + '\', '
                empresa_ente_federativo_responsavel = str(campos_cnpj['empresa_ente_federativo_responsavel']).replace('\'', '')
                sql_insert = sql_insert + '\'' + empresa_ente_federativo_responsavel + '\', '

                #Realizar consulta complementar simples:
                empresa_cnpj = str(campos_cnpj['empresa_cnpj'])
                sql_simples = f'SELECT * from simples where cnpj_basico = \'{empresa_cnpj}\''
                cursor.execute(sql_simples)
                results = cursor.fetchall()
                if results is not None and results != []:
                    campos_simples = results[0]
                    if campos_simples is not None:
                        sql_insert = sql_insert + '\'' + str(campos_simples['opcao_pelo_simples']) + '\', '
                        sql_insert = sql_insert + '\'' + str(campos_simples['data_opcao_pelo_simples']) + '\', '
                        sql_insert = sql_insert + '\'' + str(campos_simples['data_exclusao_pelo_simples']) + '\', '
                        sql_insert = sql_insert + '\'' + str(campos_simples['opcao_pelo_mei']) + '\', '
                        sql_insert = sql_insert + '\'' + str(campos_simples['data_opcao_pelo_mei']) + '\', '
                        sql_insert = sql_insert + '\'' + str(campos_simples['data_exclusao_pelo_mei']) + '\', '
                else:
                    # Se não encontrou informações sobre o simples, deixa em branco
                    sql_insert = sql_insert + '\'\', '
                    sql_insert = sql_insert + '\'\', '
                    sql_insert = sql_insert + '\'\', '
                    sql_insert = sql_insert + '\'\', '
                    sql_insert = sql_insert + '\'\', '
                    sql_insert = sql_insert + '\'\', '

                # Realizar a consulta complementar para o cnae
                cnae_codigo = str(campos_cnpj['estabelecimento_cnae_fiscal'])
                if cnae_codigo != '':
                    # SQL a ser realizada para buscar as informações do país
                    sql_cnae = f'select * from cnae where codigo = \'{cnae_codigo}\''
                    cursor.execute(sql_cnae)
                    results = cursor.fetchall()
                    if results is not None and results != []:
                        # SHould be a single result, fetch first
                        campos_cnae = results[0]
                        if campos_cnae is not None:
                            cnae_descricao = str(campos_cnae['descricao'])
                            cnae_cod_desc = f'{cnae_codigo} - {cnae_descricao}'
                        else:
                            cnae_cod_desc = cnae_cod
                        sql_insert = sql_insert + f'\'{cnae_cod_desc}\', '
                    else:
                        print(f'Erro! CNAE {cnae_codigo} não encontrado')
                        sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnae_fiscal']) + '\', '
                else:
                    sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnae_fiscal']) + '\', '

                # Realizar consulta à tabela país, caso o código do país não seja None, evitando cancelamento da query em INNER JOIN com pais com codigo None
                pais_codigo = str(campos_cnpj['estabelecimento_pais'])
                if pais_codigo != '':
                    # Brasil
                    if pais_codigo == 'None':
                        pais_codigo = '105'
                    else:
                        # Remove '.0'
                        pais_codigo = pais_codigo[:-2]
                    # SQL a ser realizada para buscar as informações do país
                    sql_pais = f'select * from pais where codigo = \'{pais_codigo}\''
                    cursor.execute(sql_pais)
                    results = cursor.fetchall()
                    if results is not None and results != []:
                        # SHould be a single result, fetch first
                        campos_pais = results[0]
                        if campos_pais is not None:
                            pais_descricao = str(campos_pais['descricao'])
                            pais_cod_desc = f'{pais_codigo} - {pais_descricao}'
                        else:
                            pais_cod_desc = pais_cod
                        sql_insert = sql_insert + f'\'{pais_cod_desc}\', '
                    else:
                        print(f'Erro! País {pais_codigo} não encontrado')
                        sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_pais']) + '\', '
                else:
                    sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_pais']) + '\', '

                # Realizar consulta para busca dos municípios:
                municipio_codigo = str(campos_cnpj['estabelecimento_municipio'])
                if (municipio_codigo != '') and (pais_codigo != 'None'):
                    sql_municipio = f'select * from municipio where codigo = \'{municipio_codigo}\''
                    if cursor is not None:
                        cursor.execute(sql_municipio)
                        results = cursor.fetchall()
                        if results is not None and results != []:
                            # SHould be a single result, fetch first
                            campos_municipio = results[0]
                            if campos_municipio is not None:
                                municipio_descricao = str(campos_municipio['descricao']).replace('\'','`')
                                municipio_cod_desc = f'{municipio_codigo} - {municipio_descricao}'
                            else:
                                municipio_cod_desc = municipio_cod
                            sql_insert = sql_insert + f'\'{municipio_cod_desc}\' '
                        else:
                            print(f'Erro! Município {municipio_codigo} não encontrado.')
                            sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_municipio']) + '\' '
                else:
                    sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_municipio']) + '\' '
                sql_insert = sql_insert + ')'
                cursor.execute(sql_insert)
    else:
        print("Cursor is None")
    pass

def conecta():
    try:
        conn = psycopg2.connect(database='qd_receita', user='postgres', password='change_here', host='localhost', port='5432', cursor_factory=RealDictCursor)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(e)

if __name__ == "__main__":
    # Estabelece conexão com o BD
    conn = conecta()
    if conn is not None:
        cursor = conn.cursor()
        # Carrega a informação de testes do CNPJ :
        #process_resposta_cnpjs('3727664', cursor)
        #process_resposta_cnpjs('16212670', cursor)
        #process_resposta_cnpjs('8682325', cursor)
        #process_resposta_cnpjs('16195097', cursor)
        #process_resposta_cnpjs('5478121', cursor)
        #process_resposta_cnpjs('8961647', cursor)
        #process_resposta_cnpjs('9206832', cursor)
        #process_resposta_cnpjs('10169300', cursor)
        #process_resposta_cnpjs('10099135', cursor)
        #process_resposta_cnpjs('10483698', cursor)
        
        # Total de CNPJs a processar:
        n_total_cnpjs = get_n_total_cnpj(cursor)
        n_total_cnpjs = int(n_total_cnpjs)
        print(f'Total de CNPJs a processar: {n_total_cnpjs}')
        # Lista de todos os cnpjs a serem processados:
        # contador de blocos de CNPJs
        offset = 0
        block_limit = 10000
        while ( offset < n_total_cnpjs):
            lista_cnpj = get_all_cnpj_ids(cursor, offset, block_limit)
            for cnpj in lista_cnpj:
                process_resposta_cnpjs(cnpj['empresa_cnpj'], cursor)
            # Incrementa o bloco
            offset = offset + block_limit
        # Encerra a conexão com o BD
        conn.commit()
        conn.close()
