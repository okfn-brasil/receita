import psycopg2

# Processar a partir de um CNPJ as tabelas relacionadas ao estabelecimento e seus socios
def process_resposta_cnpjs(cnpj_basico: str):
    # Executa uma consulta ao banco para retornar com join as informações das tabelas complementares e montar o registro a ser salvo na tabela resposta_cnpj
    sql = 'SELECT estabelecimento.cnpj_basico as estabelecimento_cnpj_basico'
    sql = sql + ' estabelecimento.cnpj_ordem as estabelecimento_cnpj_ordem'
    sql = sql + ' estabelecimento.cnpj_dv as estabelecimento_cnpj_dv'
    sql = sql + ' estabelecimento.identificador_matriz_filial as estabelecimento_identificador_matriz_filial'
    sql = sql + ' estabelecimento.nome_fantasia as estabelecimento_nome_fantasia'
    sql = sql + ' estabelecimento.situacao_cadastral as estabelecimento_situacao_cadastral'
    sql = sql + ' estabelecimento.data_situacao_cadastral as estabelecimento_data_situacao_cadastral'
    sql = sql + ' estabelecimento.motivo_situacao_cadastral as estabelecimento_motivo_situacao_cadastral'
    sql = sql + ' estabelecimento.nome_cidade_exterior as estabelecimento_nome_cidade_exterior'
    sql = sql + ' estabelecimento.pais as estabelecimento_pais'
    sql = sql + ' estabelecimento.data_inicio_atividade as estabelecimento_data_inicio_atividade'
    sql = sql + ' estabelecimento.cnae_fiscal as estabelecimento_cnae_fiscal'
    sql = sql + ' estabelecimento.cnae_fiscal_secundario as estabelecimento_cnae_fiscal_secundario'
    sql = sql + ' estabelecimento.tipo_logradouro as estabelecimento_tipo_logradouro'
    sql = sql + ' estabelecimento.logradouro as estabelecimento_logradouro'
    sql = sql + ' estabelecimento.numero as estabelecimento_numero'
    sql = sql + ' estabelecimento.complemento as estabelecimento_complemento'
    sql = sql + ' estabelecimento.bairro as estabelecimento_bairro'
    sql = sql + ' estabelecimento.cep as estabelecimento_cep'
    sql = sql + ' estabelecimento.uf as estabelecimento_uf'
    sql = sql + ' estabelecimento.municipio as estabelecimento_municipio'
    sql = sql + ' estabelecimento.ddd_1 as estabelecimento_ddd_1'
    sql = sql + ' estabelecimento.telefone_1 as estabelecimento_telefone_1'
    sql = sql + ' estabelecimento.ddd_2 as estabelecimento_ddd_2'
    sql = sql + ' estabelecimento.telefone_2 as estabelecimento_telefone_2'
    sql = sql + ' estabelecimento.ddd_fax as estabelecimento_ddd_fax'
    sql = sql + ' estabelecimento.telefone_fax as estabelecimento_telefone_fax'
    sql = sql + ' estabelecimento.correio_eletronico as estabelecimento_correio_eletronico'
    sql = sql + ' estabelecimento.situacao_especial as estabelecimento_situacao_especial'
    sql = sql + ' estabelecimento.data_situacao_especial as estabelecimento_data_situacao_especial'
    sql = sql + ' empresa.razao_social as empresa_razao_social'
    sql = sql + ' empresa.codigo_natureza_juridica as empresa_codigo_natureza_juridica'
    sql = sql + ' empresa.qualificacao_do_responsavel as empresa_qualificacao_do_responsavel'
    sql = sql + ' empresa.capital_social  as empresa_capital_social'
    sql = sql + ' empresa.porte as empresa_porte'
	sql = sql + ' empresa.ente_federativo_responsavel as empresa_ente_federativo_responsavel'
    sql = sql + ' simples.opcao_pelo_simples as simples_opcao_pelo_simples'
    sql = sql + ' simples.data_opcao_pelo_simples as simples_data_opcao_pelo_simples'
    sql = sql + ' simples.data_exclusao_pelo_simples as simples_data_exclusao_pelo_simples'
    sql = sql + ' simples.data_exclusao_pelo_mei as simples_data_exclusao_pelo_mei'
    sql = sql + ' FROM estabelecimento'
    sql = sql + ' INNER JOIN empresa ON estabelecimento.cnpj_basico = empresa.cnpj'
    sql = sql + ' INNER JOIN simples ON estabelecimento.cnpj_basico = simples.cnpj_basico'
    sql = sql + f' WHERE estabelecimento.cnpj_basico =  \'{cnpj_basico}\';'
    print(f'SQL a ser executado: \n {sql}')
    if cursor is not None:
        cursor.execute(sql)
        results = cursor.fetchall()
        if results is not None:
            print(f'A busca retornou {len(results)} resultados:')
            for r in results:
                print('************************************************')
                print(f'estabelecimento_cnae_fiscal: {r.estabelecimento_cnae_fiscal}')
                print(f'estabelecimento_pais: {r.estabelecimento_pais}')
                print(f'estabelecimento_municipio: {r.estabelecimento_municipio}')
                print(f'empresa_codigo_natureza_juridica: {r.empresa_codigo_natureza_juridica}')
    pass

def conecta():
    try:
        conn = psycopg2.connect(database='qd_receita', user='postgres', password='change_here', host='localhost', port='5432')
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
        process_resposta_cnpjs('5401158', cursor)
        # Encerra a conexão com o BD
        conn.commit()
        conn.close()
