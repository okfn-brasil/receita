import psycopg2
from psycopg2.extras import RealDictCursor

# Processar a partir de um CNPJ as tabelas relacionadas ao estabelecimento e seus socios
def process_resposta_cnpjs(cnpj_basico: str, cursor=None):
    # Executa uma consulta ao banco para retornar com join as informações das tabelas complementares e montar o registro a ser salvo na tabela resposta_cnpj
    sql = 'SELECT estabelecimento.cnpj_basico as estabelecimento_cnpj_basico,'
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
    sql = sql + ' estabelecimento.data_situacao_especial as estabelecimento_data_situacao_especial,'
    sql = sql + ' empresa.razao_social as empresa_razao_social,'
    sql = sql + ' empresa.codigo_natureza_juridica as empresa_codigo_natureza_juridica,'
    sql = sql + ' empresa.qualificacao_do_responsavel as empresa_qualificacao_do_responsavel,'
    sql = sql + ' empresa.capital_social  as empresa_capital_social,'
    sql = sql + ' empresa.porte as empresa_porte,'
    sql = sql + ' empresa.ente_federativo_responsavel as empresa_ente_federativo_responsavel,'
    '''
    # Removendo campos simples
    sql = sql + ' simples.opcao_pelo_simples as simples_opcao_pelo_simples,'
    sql = sql + ' simples.data_opcao_pelo_simples as simples_data_opcao_pelo_simples,'
    sql = sql + ' simples.data_exclusao_pelo_simples as simples_data_exclusao_pelo_simples,'
    sql = sql + ' simples.opcao_pelo_mei as simples_opcao_pelo_mei,'
    sql = sql + ' simples.data_opcao_pelo_mei as simples_data_opcao_pelo_mei,'
    sql = sql + ' simples.data_exclusao_pelo_mei as simples_data_exclusao_pelo_mei,'
    '''
    sql = sql + ' cnae.codigo as cnae_codigo,'
    sql = sql + ' cnae.descricao as cnae_descricao,'
    # TODO: a inclusão da consulta para país está pegando resultados null e cancelando a query inteira, por hora será desabilitada'
    #sql = sql + ' pais.codigo as pais_codigo,'
    #sql = sql + ' pais.descricao as pais_descricao,'
    sql = sql + ' municipio.codigo as municipio_codigo,'
    sql = sql + ' municipio.descricao as municipio_descricao'
    sql = sql + ' FROM estabelecimento'
    sql = sql + ' INNER JOIN empresa ON estabelecimento.cnpj_basico = empresa.cnpj'
    # TODO: remover e substituir por consulta complementar pras informações do simples
    #sql = sql + ' INNER JOIN simples ON estabelecimento.cnpj_basico = simples.cnpj_basico'
    sql = sql + ' INNER JOIN cnae ON estabelecimento.cnae_fiscal = cnae.codigo'
    sql = sql + ' INNER JOIN municipio ON estabelecimento.municipio = municipio.codigo'
    # sql = sql + ' INNER JOIN pais ON estabelecimento.pais = pais.codigo'
    sql = sql + f' WHERE estabelecimento.cnpj_basico =  \'{cnpj_basico}\';'
    print(f'SQL a ser executado: \n {sql}')
    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        # Armazena os campos a serem salvos na tabela de resposta_cnpj
        campos_cnpj = {}
        if results is not None:
            print(f'A busca retornou {len(results)} resultados:')
            for r in results:
                campos_cnpj = r.copy()
                print(campos_cnpj)
                # TODO: Salvar registro resposta_cnpj
                sql_insert = 'INSERT into resposta_cnpj (estabelecimento_cnpj_basico, estabelecimento_cnpj_ordem, estabelecimento_cnpj_dv, estabelecimento_identificador_matriz_filial, estabelecimento_nome_fantasia, estabelecimento_situacao_cadastral, estabelecimento_data_situacao_cadastral, estabelecimento_motivo_situacao_cadastral, estabelecimento_nome_cidade_exterior, estabelecimento_data_inicio_atividade, estabelecimento_cnae_fiscal_secundario, estabelecimento_tipo_logradouro, estabelecimento_logradouro, estabelecimento_numero, estabelecimento_complemento, estabelecimento_bairro, estabelecimento_cep, estabelecimento_uf, estabelecimento_municipio, estabelecimento_ddd_telefone_1, estabelecimento_ddd_telefone_2, estabelecimento_ddd_telefone_fax, estabelecimento_correio_eletronico, estabelecimento_situacao_especial, estabelecimento_data_situacao_especial, empresa_razao_social, empresa_codigo_natureza_juridica, empresa_qualificacao_do_responsavel, empresa_capital_social, empresa_porte, empresa_ente_federativo_responsavel, simples_opcao_pelo_simples, simples_data_opcao_pelo_simples, simples_data_exclusao_pelo_simples, simples_opcao_pelo_mei, simples_data_opcao_pelo_mei, simples_data_exclusao_pelo_mei, cnae, pais, municipio)'
                sql_insert = sql_insert + ' VALUES ('
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnpj_basico']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnpj_ordem']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnpj_dv']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_identificador_matriz_filial']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_nome_fantasia']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_situacao_cadastral']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_data_situacao_cadastral']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_motivo_situacao_cadastral']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_nome_cidade_exterior']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_data_inicio_atividade']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cnae_fiscal_secundario']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_tipo_logradouro']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_logradouro']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_numero']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_complemento']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_bairro']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_cep']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_uf']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_municipio']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_ddd_1']) + str(campos_cnpj['estabelecimento_telefone_1']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_ddd_2']) + str(campos_cnpj['estabelecimento_telefone_2']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_ddd_fax']) + str(campos_cnpj['estabelecimento_telefone_fax']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_correio_eletronico']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_situacao_especial']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_data_situacao_especial']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_razao_social']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_codigo_natureza_juridica']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_qualificacao_do_responsavel']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_capital_social']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_porte']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['empresa_ente_federativo_responsavel']) + '\', '

                #Realizar consulta complementar simples:
                sql_simples = 'SELECT '
                sql_simples = sql_simples + ' opcao_pelo_simples as simples_opcao_pelo_simples,'
                sql_simples = sql_simples + ' data_opcao_pelo_simples as simples_data_opcao_pelo_simples,'
                sql_simples = sql_simples + ' data_exclusao_pelo_simples as simples_data_exclusao_pelo_simples,'
                sql_simples = sql_simples + ' opcao_pelo_mei as simples_opcao_pelo_mei,'
                sql_simples = sql_simples + ' data_opcao_pelo_mei as simples_data_opcao_pelo_mei,'
                sql_simples = sql_simples + ' data_exclusao_pelo_mei as simples_data_exclusao_pelo_mei '
                sql_simples = sql_simples + f' from simples where cnpj_basico= \'{str(campos_cnpj['estabelecimento_cnpj_basico'])}\''
                print(f'SQL: {sql_simples}')
                if cursor is not None:
                    cursor.execute(sql_simples)
                    results = cursor.fetchall()
                    if results is not None:
                        resultado_simples = results[0]
                        if resultado_simples is not None:
                            sql_insert = sql_insert + '\'' + str(resultado_simples['opcao_pelo_simples']) + '\', '
                            sql_insert = sql_insert + '\'' + str(resultado_simples['data_opcao_pelo_simples']) + '\', '
                            sql_insert = sql_insert + '\'' + str(resultado_simples['data_exclusao_pelo_simples']) + '\', '
                            sql_insert = sql_insert + '\'' + str(resultado_simples['opcao_pelo_mei']) + '\', '
                            sql_insert = sql_insert + '\'' + str(resultado_simples['data_opcao_pelo_mei']) + '\', '
                            sql_insert = sql_insert + '\'' + str(resultado_simples['data_exclusao_pelo_mei']) + '\', '
                    else:
                        # Se não encontrou informações sobre o simples, deixa em branco
                        sql_insert = sql_insert + '\'\', '
                        sql_insert = sql_insert + '\'\', '
                        sql_insert = sql_insert + '\'\', '
                        sql_insert = sql_insert + '\'\', '
                        sql_insert = sql_insert + '\'\', '
                        sql_insert = sql_insert + '\'\', '

                # Realizar consulta à tabela país, caso o código do país não seja None, evitando cancelamento da query em INNER JOIN com pais com codigo None
                pais_codigo = str(campos_cnpj['estabelecimento_pais'])
                if (pais_codigo != '') and (pais_codigo != 'None'):
                    sql_pais = f'select * from pais where codigo = \'{pais_codigo}\''
                    if cursor is not None:
                        cursor.execute(sql_pais)
                        results = cursor.fetchall()
                        if results is not None:
                            # SHould be a single result, fetch first
                            resultado_pais = results[0]
                            pais_descricao = resultado_pais['descricao']
                            pais_cod_desc = f'{pais_codigo} - {pais_descricao}'
                            print(f'pais: {pais_cod_desc}')
                            sql_insert = sql_insert + f'\'{pais_cod_desc}\', '
                else:
                    sql_insert = sql_insert + '\'' + str(campos_cnpj['estabelecimento_pais']) + '\', '
                sql_insert = sql_insert + '\'' + str(campos_cnpj['municipio_codigo']) + ' - ' + str(campos_cnpj['municipio_descricao']) + '\''
                sql_insert = sql_insert + ')'
                print(f'SQL Insert: \n{sql_insert}')
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
        # Carrega a informação do CNPJ e dos sócios:
        process_resposta_cnpjs('3727664', cursor)
        process_resposta_cnpjs('16212670', cursor)
        process_resposta_cnpjs('8682325', cursor)
        process_resposta_cnpjs('16195097', cursor)
        process_resposta_cnpjs('5478121', cursor)
        process_resposta_cnpjs('8961647', cursor)
        process_resposta_cnpjs('9206832', cursor)
        process_resposta_cnpjs('10169300', cursor)
        process_resposta_cnpjs('10099135', cursor)
        process_resposta_cnpjs('10483698', cursor)
        # Encerra a conexão com o BD
        conn.commit()
        conn.close()
