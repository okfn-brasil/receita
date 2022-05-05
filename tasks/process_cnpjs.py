import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import extras

def get_n_total_cnpj(cursor=None):
    # Executa uma consulta para pegar a quantidade total de CNPJs a processar
    sql = 'SELECT count(cnpj) as count_cnpj from empresa;'
    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        if results is not None:
            return results[0]['count_cnpj']

# Retorna um objeto com a resposta do banco
def get_all_cnpj_ids(cursor=None, offset=0, limit=10000):
    print(f'Offset: {offset} | Limit: {limit}')
    # Executa uma consulta para pegar todos os identificadores únicos de CNPJ que serão utilizados nas buscas:
    sql = f'SELECT empresa.cnpj as empresa_cnpj from empresa order by cnpj limit {limit} offset {offset}'
    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        if results is not None:
            return results

def batch_insert_resposta_cnpj(cursor, resposta_cnpj_lista_valores):
    # Consulta adaptada para inserção via execute_batch
    sql_insert = 'INSERT into resposta_cnpj (estabelecimento_cnpj_basico, estabelecimento_cnpj_ordem, estabelecimento_cnpj_dv, estabelecimento_identificador_matriz_filial, estabelecimento_nome_fantasia, estabelecimento_situacao_cadastral, estabelecimento_data_situacao_cadastral, estabelecimento_motivo_situacao_cadastral, estabelecimento_nome_cidade_exterior, estabelecimento_data_inicio_atividade, estabelecimento_cnae_fiscal_secundario, estabelecimento_tipo_logradouro, estabelecimento_logradouro, estabelecimento_numero, estabelecimento_complemento, estabelecimento_bairro, estabelecimento_cep, estabelecimento_uf, estabelecimento_ddd_telefone_1, estabelecimento_ddd_telefone_2, estabelecimento_ddd_telefone_fax, estabelecimento_correio_eletronico, estabelecimento_situacao_especial, estabelecimento_data_situacao_especial, empresa_razao_social, empresa_codigo_natureza_juridica, empresa_qualificacao_do_responsavel, empresa_capital_social, empresa_porte, empresa_ente_federativo_responsavel, simples_opcao_pelo_simples, simples_data_opcao_pelo_simples, simples_data_exclusao_pelo_simples, simples_opcao_pelo_mei, simples_data_opcao_pelo_mei, simples_data_exclusao_pelo_mei, cnae, pais, municipio)'
    sql_insert = sql_insert + ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    if cursor is not None and resposta_cnpj_lista_valores is not None:
        try:
            extras.execute_batch(cursor, sql_insert, resposta_cnpj_lista_valores)
            print(f'Batch insert executado salvou {len(resposta_cnpj_lista_valores)}')
        except Exception as e:
            print('Erro de inserção batch: ' + str(e))

def batch_insert_resposta_socios(cursor, lista_resposta_socios):
    # Consulta adaptada para inserção via execute_batch
    sql_insert = 'INSERT into resposta_socios (cnpj_basico, identificador_socio, razao_social, cnpj_cpf_socio, qualificacao_socio, data_entrada_sociedade, pais_socio_estrangeiro, numero_cpf_representante_legal, nome_representante_legal, codigo_qualificacao_representante_legal, faixa_etaria) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    if cursor is not None and lista_resposta_socios is not None:
        try:
            extras.execute_batch(cursor, sql_insert, lista_resposta_socios)
            print(f'Batch insert executado salvou {len(lista_resposta_socios)}')
        except Exception as e:
            print('Erro de inserção batch: ' + str(e))

# Processar a partir de um CNPJ as tabelas relacionadas ao estabelecimento
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
        resposta_cnpj = {}
        campos_cnpj = {}
        if results is not None and results != []:
            r = results[0]
            campos_cnpj = r.copy()
            # Sanitização e montagem dos campos compostos salvando num objeto único resposta_cnpj a ser submetido
            resposta_cnpj['estabelecimento_cnpj_basico'] = campos_cnpj['estabelecimento_cnpj_basico']
            resposta_cnpj['estabelecimento_cnpj_ordem'] = campos_cnpj['estabelecimento_cnpj_ordem']
            resposta_cnpj['estabelecimento_cnpj_dv'] = campos_cnpj['estabelecimento_cnpj_dv']
            resposta_cnpj['estabelecimento_identificador_matriz_filial'] = campos_cnpj['estabelecimento_identificador_matriz_filial']
            resposta_cnpj['estabelecimento_nome_fantasia'] = str(campos_cnpj['estabelecimento_nome_fantasia']).replace('\'', '`')
            resposta_cnpj['estabelecimento_situacao_cadastral'] = campos_cnpj['estabelecimento_situacao_cadastral']
            resposta_cnpj['estabelecimento_data_situacao_cadastral'] = campos_cnpj['estabelecimento_data_situacao_cadastral']
            resposta_cnpj['estabelecimento_motivo_situacao_cadastral'] = campos_cnpj['estabelecimento_motivo_situacao_cadastral']
            resposta_cnpj['estabelecimento_nome_cidade_exterior'] = str(campos_cnpj['estabelecimento_nome_cidade_exterior']).replace('\'', '`')
            resposta_cnpj['estabelecimento_data_inicio_atividade'] = campos_cnpj['estabelecimento_data_inicio_atividade']
            resposta_cnpj['estabelecimento_cnae_fiscal_secundario'] = campos_cnpj['estabelecimento_cnae_fiscal_secundario']
            resposta_cnpj['estabelecimento_tipo_logradouro'] = campos_cnpj['estabelecimento_tipo_logradouro']
            resposta_cnpj['estabelecimento_logradouro'] = str(campos_cnpj['estabelecimento_logradouro']).replace('\'', '`')
            resposta_cnpj['estabelecimento_numero'] = str(campos_cnpj['estabelecimento_numero']).replace('\'', '')
            resposta_cnpj['estabelecimento_complemento'] = str(campos_cnpj['estabelecimento_complemento']).replace('\'', '`')
            resposta_cnpj['estabelecimento_bairro'] = str(campos_cnpj['estabelecimento_bairro']).replace('\'', '`')
            resposta_cnpj['estabelecimento_cep'] = campos_cnpj['estabelecimento_cep']
            resposta_cnpj['estabelecimento_uf'] = campos_cnpj['estabelecimento_uf']
            # Tratando campos de telefone
            estabelecimento_ddd_telefone_1 = str(campos_cnpj['estabelecimento_ddd_1'])[:-2] + str(campos_cnpj['estabelecimento_telefone_1'])
            if 'None' in estabelecimento_ddd_telefone_1:
                estabelecimento_ddd_telefone_1 = ''
            resposta_cnpj['estabelecimento_ddd_telefone_1'] = estabelecimento_ddd_telefone_1

            estabelecimento_ddd_telefone_2 = str(campos_cnpj['estabelecimento_ddd_2'])[:-2] + str(campos_cnpj['estabelecimento_telefone_2'])
            if 'None' in estabelecimento_ddd_telefone_2:
                estabelecimento_ddd_telefone_2 = ''
            resposta_cnpj['estabelecimento_ddd_telefone_2'] = estabelecimento_ddd_telefone_2

            estabelecimento_ddd_telefone_fax = str(campos_cnpj['estabelecimento_ddd_fax'])[:-2] + str(campos_cnpj['estabelecimento_telefone_fax'])
            if 'None' in estabelecimento_ddd_telefone_fax:
                estabelecimento_ddd_telefone_fax = ''
            resposta_cnpj['estabelecimento_ddd_telefone_fax'] = estabelecimento_ddd_telefone_fax
            estabelecimento_correio_eletronico = str(campos_cnpj['estabelecimento_correio_eletronico'])
            estabelecimento_correio_eletronico = estabelecimento_correio_eletronico.replace('\"', '')
            estabelecimento_correio_eletronico = estabelecimento_correio_eletronico.replace('\'', '')
            resposta_cnpj['estabelecimento_correio_eletronico'] = estabelecimento_correio_eletronico
            resposta_cnpj['estabelecimento_situacao_especial'] = campos_cnpj['estabelecimento_situacao_especial']
            resposta_cnpj['estabelecimento_data_situacao_especial'] = campos_cnpj['estabelecimento_data_situacao_especial']
            resposta_cnpj['empresa_razao_social'] = str(campos_cnpj['empresa_razao_social']).replace('\'', '`')
            empresa_codigo_natureza_juridica = campos_cnpj['empresa_codigo_natureza_juridica']
            # Realizar consulta complementar para a natureza jurídica da empresa:
            sql_natureza_juridica = f'SELECT * from natureza_juridica where codigo = {empresa_codigo_natureza_juridica}'
            cursor.execute(sql_natureza_juridica)
            results = cursor.fetchall()
            if results is not None and results != []:
                campos_natureza_juridica = results[0]
                if campos_natureza_juridica is None:
                    resposta_cnpj['empresa_codigo_natureza_juridica'] = ''
                else:
                    resposta_cnpj['empresa_codigo_natureza_juridica'] = str(empresa_codigo_natureza_juridica) + ' - ' + campos_natureza_juridica['descricao']
            resposta_cnpj['empresa_qualificacao_do_responsavel'] = campos_cnpj['empresa_qualificacao_do_responsavel']
            resposta_cnpj['empresa_capital_social'] = campos_cnpj['empresa_capital_social']
            resposta_cnpj['empresa_porte'] = campos_cnpj['empresa_porte']
            resposta_cnpj['empresa_ente_federativo_responsavel'] = str(campos_cnpj['empresa_ente_federativo_responsavel']).replace('\'', '')
            # Realizar consulta complementar simples:
            empresa_cnpj = str(campos_cnpj['empresa_cnpj'])
            sql_simples = f'SELECT * from simples where cnpj_basico = \'{empresa_cnpj}\''
            cursor.execute(sql_simples)
            results = cursor.fetchall()
            if results is not None and results != []:
                campos_simples = results[0]
                if campos_simples is None:
                    resposta_cnpj['simples_opcao_pelo_simples'] = ''
                    resposta_cnpj['simples_data_opcao_pelo_simples'] = ''
                    resposta_cnpj['simples_data_exclusao_pelo_simples'] = ''
                    resposta_cnpj['simples_opcao_pelo_mei'] = ''
                    resposta_cnpj['simples_data_opcao_pelo_mei'] = ''
                    resposta_cnpj['simples_data_exclusao_pelo_mei'] = ''
                else:
                    resposta_cnpj['simples_opcao_pelo_simples'] = campos_simples['opcao_pelo_simples']
                    resposta_cnpj['simples_data_opcao_pelo_simples'] = campos_simples['data_opcao_pelo_simples']
                    resposta_cnpj['simples_data_exclusao_pelo_simples'] = campos_simples['data_exclusao_pelo_simples']
                    resposta_cnpj['simples_opcao_pelo_mei'] = campos_simples['opcao_pelo_mei']
                    resposta_cnpj['simples_data_opcao_pelo_mei'] = campos_simples['data_opcao_pelo_mei']
                    resposta_cnpj['simples_data_exclusao_pelo_mei'] = campos_simples['data_exclusao_pelo_mei']
            else:
                resposta_cnpj['simples_opcao_pelo_simples'] = ''
                resposta_cnpj['simples_data_opcao_pelo_simples'] = ''
                resposta_cnpj['simples_data_exclusao_pelo_simples'] = ''
                resposta_cnpj['simples_opcao_pelo_mei'] = ''
                resposta_cnpj['simples_data_opcao_pelo_mei'] = ''
                resposta_cnpj['simples_data_exclusao_pelo_mei'] = ''

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
                    cnae_cod_desc = ''
                    if campos_cnae is not None:
                        cnae_descricao = str(campos_cnae['descricao'])
                        cnae_cod_desc = f'{cnae_codigo} - {cnae_descricao}'
                    else:
                        cnae_cod_desc = cnae_codigo
                    resposta_cnpj['cnae'] = cnae_cod_desc
                else:
                    print(f'Erro! CNAE {cnae_codigo} não encontrado')
                    resposta_cnpj['cnae'] =  '\'' + str(campos_cnpj['estabelecimento_cnae_fiscal']) + '\''
            else:
                resposta_cnpj['cnae'] = '\'' + str(campos_cnpj['estabelecimento_cnae_fiscal']) + '\''

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
                    resposta_cnpj['pais'] = f'\'{pais_cod_desc}\''
                else:
                    print(f'Erro! País {pais_codigo} não encontrado')
                    resposta_cnpj['pais'] = '\'' + str(campos_cnpj['estabelecimento_pais']) + '\''
            else:
                resposta_cnpj['pais'] = '\'' + str(campos_cnpj['estabelecimento_pais']) + '\''

            # Realizar consulta para busca do município:
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
                        resposta_cnpj['municipio'] = f'\'{municipio_cod_desc}\' '
                    else:
                        print(f'Erro! Município {municipio_codigo} não encontrado.')
                        resposta_cnpj['municipio'] = '\'' + str(campos_cnpj['estabelecimento_municipio']) + '\''
            else:
                resposta_cnpj['municipio'] = '\'' + str(campos_cnpj['estabelecimento_municipio']) + '\''
            return resposta_cnpj
    else:
        print("Cursor is None")
        return None
    pass


# Processar a partir de um CNPJ as tabelas relacionadas aos sócios da empresa
def process_resposta_socios(cnpj_basico: str, cursor=None):
    # Executa uma consulta ao banco para retornar com join as informações das tabelas complementares e montar o registro a ser salvo na tabela resposta_cnpj
    sql = 'SELECT socio.cnpj_basico as socio_cnpj_basico,'
    sql = sql + ' socio.identificador_socio as socio_identificador_socio,'
    sql = sql + ' socio.razao_social as socio_razao_social,'
    sql = sql + ' socio.cnpj_cpf_socio as socio_cnpj_cpf_socio,'
    sql = sql + ' socio.codigo_qualificacao_socio as socio_codigo_qualificacao_socio,'
    sql = sql + ' socio.data_entrada_sociedade as socio_data_entrada_sociedade,'
    sql = sql + ' socio.codigo_pais_socio_estrangeiro as socio_codigo_pais_socio_estrangeiro,'
    sql = sql + ' socio.numero_cpf_representante_legal as socio_numero_cpf_representante_legal,'
    sql = sql + ' socio.nome_representante_legal as socio.nome_representante_legal,'
    sql = sql + ' socio.codigo_qualificacao_representante_legal as socio_codigo_qualificacao_representante_legal,'
    sql = sql + ' socio.faixa_etaria as socio_faixa_etaria'
    sql = sql + ' FROM socio'
    sql = sql + ' INNER JOIN empresa ON socio.cnpj_basico = empresa.cnpj'
    sql = sql + f' WHERE empresa.cnpj =  \'{cnpj_basico}\';'

    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        # Armazena os campos a serem salvos na tabela de resposta_socios
        resposta_socios = {}
        campos_socios = {}
        lista_resposta_socios = []

        if results is not None and results != []:
            for r in results:
                campos_socios = r.copy()
                # Sanitização e montagem dos campos compostos salvando uma linha para cada sócio do CNPJ a ser submetida na tabela resposta_socios
                resposta_socios['socio_cnpj_basico'] = campos_socios['socio_cnpj_basico']
                resposta_socios['socio_identificador_socio'] = campos_socios['socio_identificador_socio']
                resposta_socios['socio_razao_social'] = campos_socios['socio_razao_social']
                resposta_socios['socio_cnpj_cpf_socio'] = campos_socios['socio_cnpj_cpf_socio']
                # TODO run other select to try to fetch data
                socio_codigo_qualificacao_socio = campos_socios['socio_codigo_qualificacao_socio']
                if socio_codigo_qualificacao_socio is not None:
                    sql_qualificacao_socio = f'SELECT from qualificacao_socio where codigo = \'{socio_codigo_qualificacao_socio}\''
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    if results is not None and results != []:
                        r = results[0]
                        if r is not None:
                            socio_codigo_qualificacao_socio = socio_codigo_qualificacao_socio + ' - ' + r['descricao']
                            resposta_socios['socio_codigo_qualificacao_socio'] = socio_codigo_qualificacao_socio
                resposta_socios['socio_data_entrada_sociedade'] = campos_socios['socio_data_entrada_sociedade']

                # Realizar consulta à tabela país, caso o código do país não seja None, evitando cancelamento da query em INNER JOIN com pais com codigo None
                pais_codigo = str(campos_socios['socio_codigo_pais_socio_estrangeiro'])
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
                        resposta_socios['pais_socio_estrangeiro'] = f'\'{pais_cod_desc}\''
                    else:
                        print(f'Erro! País {pais_codigo} não encontrado')
                        resposta_socios['pais_socio_estrangeiro'] = '\'' + pais_codigo + '\''
                else:
                    resposta_socios['pais_socio_estrangeiro'] = '\'' + str(campos_socios['socio_codigo_pais_socio_estrangeiro']) + '\''

                resposta_socios['socio_numero_cpf_representante_legal'] = campos_socios['socio_numero_cpf_representante_legal']
                resposta_socios['nome_representante_legal'] = campos_socios['nome_representante_legal']
                resposta_socios['socio_codigo_qualificacao_representante_legal'] = campos_socios['socio_codigo_qualificacao_representante_legal']
                resposta_socios['socio_faixa_etaria'] = campos_socios['socio_faixa_etaria']
                lista_resposta_socios.append(list(resposta_socios.values()))
            batch_insert_resposta_socios(cursor, lista_resposta_socios)
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
        # Total de CNPJs a processar:
        n_total_cnpjs = get_n_total_cnpj(cursor)
        n_total_cnpjs = int(n_total_cnpjs)
        print(f'Total de CNPJs a processar: {n_total_cnpjs}')
        # Lista de todos os cnpjs a serem processados:
        # contador de blocos de CNPJs
        offset = 0
        block_size = 10000
        # lista de objetos a serem salvos em uma FANTASIA
        while ( offset < n_total_cnpjs):
            lista_resposta_cnpj = []
            # Pega uma lista de cnpjs da fatia
            lista_cnpj = get_all_cnpj_ids(cursor, offset, block_size)
            print(f'Processando {len(lista_cnpj)} registros')
            for cnpj in lista_cnpj:
                resposta_cnpj = process_resposta_cnpjs(cnpj['empresa_cnpj'], cursor)
                if resposta_cnpj is not None:
                    lista_resposta_cnpj.append(list(resposta_cnpj.values()))
                # O processamento da resposta_socios para o CNPJ chama internamente o método de inserção em batch para inserir de uma vez todos os sócios relacionados ao CNPJ
                process_resposta_socios(cnpj['empresa_cnpj'], cursor)
            # Insere os registros do bloco no BANCO
            batch_insert_resposta_cnpj(cursor, lista_resposta_cnpj)
            # Incrementa o bloco
            offset = offset + block_size
        # Encerra a conexão com o BD
        conn.commit()
        conn.close()
