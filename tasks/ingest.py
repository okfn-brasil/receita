# Deve ser executado de dentro do diretório task/
# Para cada tabela/tema (ex.: socio, estabelecimento, cnae, empresa, etc.):
# Descompactar os arquivos .zip da tabela
# Para cada .csv referente à tabela:
# i. Carregar em memória os dados do .csv (EXTRACT)
# ii. Executar qualquer tarefa de limpeza/conversão necessária (TRANSFER)
# iii. Inserir na tabela apropriada do banco de dados qd_receita os registros extraídos dos arquivos .csv (LOAD)

import glob
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine
import sys
import psycopg2
import time
from sqlalchemy import event
from concurrent.futures import ThreadPoolExecutor
import StringIO

# Mapeamento  entre os nomes das tabelas no banco e uma referência ao nome do arquivo em disco para os arquivos .csv e .zip
mapeamento_tabelas_zip = {
    "empresa": "EMPRE",
    "estabelecimento": "ESTABELE",
    "simples": "SIMPLES",
    "socio": "SOCIO",
    "pais": "PAIS",
    "municipio": "MUNIC",
    "qualificacoes_socio": "QUALS",
    "natureza_juridica": "NATJU",
    "cnae": "CNAE",
    #"motivo_situacao_cadastral": "MOTI"
}
# TODO: Verificar se não está baixando os arquivos "MOTI" dos .zip?

# Mapeamento campos por [índice] .csv estabelecimento → nome dos campos tabela BD
mapeamento_nome_campos = {
    'empresa': ['cnpj', 'razao_social', 'codigo_natureza_juridica',  'qualificacao_do_responsavel', 'capital_social', 'porte', 'ente_federativo_responsavel'],
    'estabelecimento': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal', 'cnae_fiscal_secundario', 'tipo_logradouro',
    'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'telefone_fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial'],
    'simples': ['cnpj_basico', 'opcao_pelo_simples', 'data_opcao_pelo_simples', 'data_exclusao_pelo_simples', 'opcao_pelo_mei', 'data_opcao_pelo_mei', 'data_exclusao_pelo_mei'],
    'socio': ['cnpj_basico', 'identificador_socio', 'razao_social', 'cnpj_cpf_socio', 'codigo_qualificacao_socio', 'data_entrada_sociedade', 'codigo_pais_socio_estrangeiro', 'numero_cpf_representante_legal', 'nome_representante_legal', 'codigo_qualificacao_representante_legal', 'faixa_etaria'],
    'pais': ['codigo', 'descricao'],
    'municipio': ['codigo', 'descricao'],
    'qualificacoes_socio': ['codigo', 'descricao'],
    'natureza_juridica': ['codigo', 'descricao'],
    'cnae': ['codigo', 'descricao']
}

# Pegar a lista de todos os nomes de arquivo .zip que tenham uma referência à string "nome_arquivo"
def pegar_lista_zip(nome_arquivo: str):
    zip_files = glob.glob(r"../data/*.zip")
    itens_tabela = []
    # Remove todos os arquivos que não tenham relação com a tabela selecionada para a carga
    for f in zip_files:
        if nome_arquivo in f:
            itens_tabela.append(f)
    # Retorna a lista completa de arquivos .zip da tabela
    return itens_tabela

# Descompactar um arquivo .zip da tabela a partir da string com referência completa ao diretório e nome do arquivo
# Retorna True caso realize a descompressão com sucesso e False, caso contrário/
def descomprimir_zip(caminho_nome_zip: str):
    try:
        with ZipFile(caminho_nome_zip, 'r') as arquivo:
            # Extrair no mesmo diretório data
            arquivo.extractall(path="../data/")
            return True
    except:
        print(f'Erro de descompressão do arquivo {caminho_nome_zip}')
    return False

# Realiza uma regra de três para calcular a porcentagem de andamento da carga dos dados
def status_carga(parcial: int, total: int):
    x = (parcial * 100 ) / total
    # Arrendondando os valores
    x = round(x)
    return str(x)

# i. Carregar em memória os dados do .csv (EXTRACT) usando o pandas e sqlalchemy
# A referência ao arquivo .csv é a string caminho_nome_arquivo, tabela é a chave da tabela em que será inserida
# Retorna True caso a operação realize o registro dos dados no banco e False caso contrário
def carregar_dados(caminho_nome_arquivo: str, tabela: str, sql_engine):
    campos_selecionados = mapeamento_nome_campos[tabela]
    try:
        read_chunk_size = 100_000
        write_chunk_size = 15_000
        # Total de itens a serem carregados do arquivo csv
        n_items_csv = 0
        # Total de itens já salvos no banco de dados
        n_carregados = 0
        # Passo intermediário para pegar o número total de itens do .csv
        with pd.read_csv(caminho_nome_arquivo, chunksize=read_chunk_size, delimiter=';', names=campos_selecionados, encoding='latin-1', on_bad_lines='warn', header=None) as csv_reader:
            for chunk in csv_reader:
                n_items_csv = n_items_csv + int(chunk.shape[0])
            print(f'Total de {n_items_csv} a serem carregados em memória')
        with pd.read_csv(caminho_nome_arquivo, chunksize=read_chunk_size, delimiter=';', names=campos_selecionados, encoding='latin-1', on_bad_lines='warn', header=None) as csv_reader:
            for chunk in csv_reader:
                # Salva no banco de dados os registros:
                if sql_engine is not None:
                    try:
                        # iii. Inserir na tabela apropriada do banco de dados qd_receita os registros extraídos dos arquivos .csv (LOAD)
                        print(f'Salvando os registros na tabela {tabela}')
                        start_insert = time.time()
                        total_records_updated = chunk.to_sql(tabela, sql_engine, index=False, if_exists='append', chunksize=write_chunk_size, method='multi')
                        # Incrementa pro monitor de desempenho da carga
                        n_carregados = n_carregados + total_records_updated
                        porcentagem = status_carga(n_carregados, n_items_csv)
                        end_insert = time.time() - start_insert
                        if end_insert < 60:
                            print(f'{total_records_updated} registros de um total de {n_items_csv} foram salvos na tabela *{tabela}* em {end_insert} segundos. {n_carregados} carregados ({porcentagem}%).')
                        else:
                            minutes = round(end_insert/60)
                            print(f'{total_records_updated} registros de um total de {n_items_csv} foram salvos na tabela *{tabela}* em {minutes} minutos. {n_carregados} carregados ({porcentagem}%).')
                    except Exception as e:
                        print(f'Erro {e} ao salvar os dados na tabela {tabela}')
            porcentagem = status_carga(n_carregados, n_items_csv)
            print(f'Foram salvos {n_carregados} de um total de {n_items_csv} ({porcentagem}).')
            return True
    except:
        exc_info = sys.exc_info()
        print(f'Erro de carga do .CSV em memória via pandas: {exc_info}')

    return False

def carregar_tabela(caminho_arquivo, nome_tabela, nome_db, host, porta, user, pwd):
    '''
    This function uploads csv to a target table using copy_from
    '''
    # Conexão com o Banco
    conn = None
    # Stream StreamIO
    sio = None
    try:
        conn = psycopg2.connect(dbname=nome_db, host=host, port=porta, user=user, password=pwd)
        # conn.autocommit = True
        print("Conectado ao BD com sucesso.")
    except Exception as e:
        print("Erro de conexão ao banco de dados: {}".format(str(e)))
        sys.exit(1)
    campos_selecionados = mapeamento_nome_campos[tabela]
    try:
        read_chunk_size = 100_000
        write_chunk_size = 15_000
        # Total de itens a serem carregados do arquivo csv
        n_items_csv = 0
        # Total de itens já salvos no banco de dados
        n_carregados = 0
        # Passo intermediário para pegar o número total de itens do .csv
        with pd.read_csv(caminho_nome_arquivo, chunksize=read_chunk_size, delimiter=';', names=campos_selecionados, encoding='latin-1', on_bad_lines='warn', header=None) as csv_reader:
            for chunk in csv_reader:
                n_items_csv = n_items_csv + int(chunk.shape[0])
            print(f'Total de {n_items_csv} a serem carregados em memória')
        with pd.read_csv(caminho_nome_arquivo, chunksize=read_chunk_size, delimiter=';', names=campos_selecionados, encoding='latin-1', on_bad_lines='warn', header=None) as csv_reader:
            for chunk in csv_reader:
                try:
                    sio = StringIO()
                    writer = csv.writer(sio)
                    writer.writerows(chunk.values)
                    sio.seek(0)
                except Exception as e:
                    print("Erro objeto de escrita StringIO: {}".format(str(e)))
                    sys.exit(1)

                if sio is not None:
                    # Grava no banco
                    try:
                        with conn.cursor() as c:
                            copy_from_response = c.copy_from(
                                file=sio,
                                table=nome_tabela,
                                columns=campos_selecionados,
                                sep=";"
                            )
                            print(f'Copy from response: {copy_from_response}')
                            commit_response = conn.commit()
                            print(f'Commit response: {commit_response}')
                    except Exception as e:
                        print("Erro de escrita no banco de dados: {}".format(str(e)))
                        sys.exit(1)
            conn.close()
            print("Conexão com o banco fechada.")

# Fluxo de execução principal do programa
def ingest_datasets():
    # Instantiate sqlachemy.create_engine object
    sql_engine = None
    try:
        sql_engine = create_engine('postgresql://postgres:q7Muz4W5iI9@localhost:5432/qd_receita', isolation_level="AUTOCOMMIT")
        print(f'Sql engine loaded {sql_engine}')

    except:
        print(f'Erro de conexão via SQLAlchemy: {sys.exc_info()}')

    # Para cada tabela/tema (ex.: socio, estabelecimento, cnae, empresa, etc.):
    for chave in sorted(mapeamento_tabelas_zip.keys(), key=str.lower, reverse=False):
        print(f'Carregando dados para a tabela {chave}')
        try:
            # Pegar lista de nomes de arquivos .zip relacionados à tabela
            lista_zip = pegar_lista_zip(mapeamento_tabelas_zip[chave])
            lista_zip = sorted(lista_zip, key=str.lower, reverse=False)
            print(f'Lista de arquivos para a tabela: \n {lista_zip}')
            for arquivo_zip in lista_zip:
                # Descompactar os arquivos .zip da tabela
                descomprimiu = False
                try:
                    print(f'Descomprinindo arquivo {arquivo_zip}')
                    descomprimiu = descomprimir_zip(f'../data/{arquivo_zip}')
                except:
                    print(f'Erro de descompressão do arquivo {arquivo_zip}')
                if descomprimiu:
                    # Carregar o csv em memória
                    try:
                        start_time = time.time()
                        print(f'Arquivo {arquivo_zip} descomprimido com sucesso. Carregando para a tabela no banco de dados...')
                        # O arquivo .csv extraído tem o mesmo nome do zip sem a extensão. Adicionando o diretório de dados ao caminho relativo
                        arquivo_csv = arquivo_zip.replace('.zip', '')
                        # Implementação multi-thread
                        t1 = time.time()
                        with ThreadPoolExecutor(max_workers=3) as processPool:
                            future_result = processPool.submit(carregar_dados, arquivo_csv, chave, sql_engine)
                            sucesso = future_result.result()
                            print(f'Resultado: {sucesso}')
                            t2 = time.time()
                            tt = t2 - t1
                            print(f'Demorou {tt} segundos')
                        # Carregar o .csv  em memória e persistir as linhas no banco de dados, usando sqlalchemy
                        # sucesso = carregar_dados(arquivo_csv, chave, sql_engine)
                        # sucesso = carregar_tabela(arquivo_csv, chave, 'qd_receita', 'localhost', '5432', 'postgres', pwd)
                        print(f'A carga dos dados demorou {(time.time() - start_time)} segundos.')
                        if sucesso:
                            print(f'Registros carregados com sucesso para o arquivo {arquivo_csv}')
                        else:
                            print(f'Erro na carga do arquivo {arquivo_csv}')
                    except:
                        exc_info = sys.exc_info()
                        print(f'Erro de carga do .csv em memória: \n{exc_info}')
            print(f'Fim da carga dos arquivos CSV para a chave {chave}')
        except:
            print(f'Erro na busca de arquivos zip para a tabela {chave}: \n \t{sys.exc_info()}')
    pass

# Set hook for main execution flow to the ingest_datasets method
if __name__ == "__main__":
    ingest_datasets()
