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
import io
import csv
import os

# Mapeamento  entre os nomes das tabelas no banco e uma referência ao nome do arquivo em disco para os arquivos .csv e .zip
mapeamento_tabelas_zip = {
    "empresa": "Empresas",
    "estabelecimento": "Estabelecimentos",
    "simples": "Simples",
    "socio": "Socios",
    "pais": "Paises",
    "municipio": "Municipios",
    "qualificacao_socio": "Qualificacoes",
    "natureza_juridica": "Naturezas",
    "cnae": "Cnaes",
}

# Mapeamento campos por [índice] .csv estabelecimento → nome dos campos tabela BD
mapeamento_nome_campos = {
    'empresa': {'cnpj': 'str', 'razao_social': 'str', 'codigo_natureza_juridica': 'str',  'qualificacao_do_responsavel': 'str', 'capital_social': 'str', 'porte': 'str', 'ente_federativo_responsavel': 'str'},
    'estabelecimento': {'cnpj_basico': 'str', 'cnpj_ordem': 'str', 'cnpj_dv': 'str', 'identificador_matriz_filial': 'str', 'nome_fantasia': 'str', 'situacao_cadastral': 'str', 'data_situacao_cadastral': 'str', 'motivo_situacao_cadastral': 'str', 'nome_cidade_exterior': 'str', 'pais': 'str', 'data_inicio_atividade': 'str', 'cnae_fiscal': 'str', 'cnae_fiscal_secundario': 'str', 'tipo_logradouro': 'str',
    'logradouro': 'str', 'numero': 'str', 'complemento': 'str', 'bairro': 'str', 'cep': 'str', 'uf': 'str', 'municipio': 'str', 'ddd_1': 'str', 'telefone_1': 'str', 'ddd_2': 'str', 'telefone_2': 'str', 'ddd_fax': 'str', 'telefone_fax': 'str', 'correio_eletronico': 'str', 'situacao_especial': 'str', 'data_situacao_especial': 'str'},
    'simples': {'cnpj_basico': 'str', 'opcao_pelo_simples': 'str', 'data_opcao_pelo_simples': 'str', 'data_exclusao_pelo_simples': 'str', 'opcao_pelo_mei': 'str', 'data_opcao_pelo_mei': 'str', 'data_exclusao_pelo_mei': 'str'},
    'socio': {'cnpj_basico': 'str', 'identificador_socio': 'str', 'razao_social': 'str', 'cnpj_cpf_socio': 'str', 'codigo_qualificacao_socio': 'str', 'data_entrada_sociedade': 'str', 'codigo_pais_socio_estrangeiro': 'str', 'numero_cpf_representante_legal': 'str', 'nome_representante_legal': 'str', 'codigo_qualificacao_representante_legal': 'str', 'faixa_etaria': 'str'},
    'pais': {'codigo': 'str', 'descricao': 'str'},
    'municipio': {'codigo': 'str', 'descricao': 'str'},
    'qualificacao_socio': {'codigo': 'str', 'descricao': 'str'},
    'natureza_juridica': {'codigo': 'str', 'descricao': 'str'},
    'cnae': {'codigo': 'str', 'descricao': 'str'}
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
    campos_selecionados = list(mapeamento_nome_campos[tabela].keys())
    tipos_selecionados = mapeamento_nome_campos[tabela]
    try:
        read_chunk_size = 100_000
        write_chunk_size = 10_000
        # Total de itens a serem carregados do arquivo csv
        n_items_csv = 0
        # Total de itens já salvos no banco de dados
        n_carregados = 0

        # Configurando datatypes para evitar erros de carga do .CSV para a memória via pandas
        data_types = {}

        # Passo intermediário para pegar o número total de itens do .csv
        with pd.read_csv(caminho_nome_arquivo, chunksize=read_chunk_size, delimiter=';', names=campos_selecionados, encoding='latin-1', on_bad_lines='warn', header=None, index_col=False, low_memory=False) as csv_reader:
            for chunk in csv_reader:
                n_items_csv = n_items_csv + int(chunk.shape[0])
            print(f'Total de {n_items_csv} a serem carregados em memória')
        with pd.read_csv(caminho_nome_arquivo, dtype=tipos_selecionados, chunksize=read_chunk_size, delimiter=';', names=campos_selecionados, encoding='latin-1', on_bad_lines='warn', header=None, index_col=False, low_memory=False) as csv_reader:
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
            print(f'Foram salvos {n_carregados} de um total de {n_items_csv} ({porcentagem}%).')
            # Deletando o arquivo CSV após a carga.
            try:
                removed = os.remove(caminho_nome_arquivo)
                if removed:
                    print(f'→ Arquivo [{caminho_nome_arquivo}] deletado com sucesso.')
                else:
                    print(f'→ Erro ao remover arquivo [{caminho_nome_arquivo}].')
            except Exception as e:
                print(f'→ Ocorreu algo inesperado: [{str(e)}].')
            return True
    except:
        exc_info = sys.exc_info()
        print(f'Erro de carga do .CSV em memória via pandas: {exc_info}')

    return False

# Fluxo de execução principal do programa
def ingest_datasets():
    # Instantiate sqlachemy.create_engine object
    sql_engine = None
    try:
        # Remember to set here
        db_password = 'change here'
        # Adding pass to the connection string
        db_url =(f'postgresql://postgres:{db_password}@localhost:5432/qd_receita')
        sql_engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
        print(f'Motor de acesso conectado com sucesso: {sql_engine}')
    except:
        print(f'Erro de conexão via SQLAlchemy: {sys.exc_info()}')

    # Para cada tabela/tema (ex.: socio, estabelecimento, cnae, empresa, etc.):
    # for chave in sorted(mapeamento_tabelas_zip.keys(), key=str.lower, reverse=False):
    for chave in ['socio']:
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
                            #future_result = processPool.submit(carregar_tabela, arquivo_csv, chave, 'qd_receita', 'localhost', '5432', 'postgres', change_password)
                            sucesso = future_result.result()
                            print(f'Resultado: {sucesso}')
                            t2 = time.time()
                            tt = t2 - t1
                            if tt > 60:
                                tt = round(tt/60)
                                print(f'Demorou {tt} minutos')
                            else:
                                print(f'Demorou {tt} segundos')
                        # Carregar o .csv  em memória e persistir as linhas no banco de dados, usando sqlalchemy
                        # sucesso = carregar_dados(arquivo_csv, chave, sql_engine)
                        # sucesso = carregar_tabela(arquivo_csv, chave, 'qd_receita', 'localhost', '5432', 'postgres', pwd)
                        tempo_carga = (time.time() - start_time)
                        if tempo_carga > 60:
                            tempo_carga = round(tempo_carga/60)
                            print(f'A carga dos dados demorou {tempo_carga} minutos.')
                        else:
                            print(f'A carga dos dados demorou {tempo_carga} segundos.')
                        if sucesso:
                            print(f'Registros carregados com sucesso para o arquivo {arquivo_csv}')
                        else:
                            print(f'Erro na carga do arquivo {arquivo_csv}')
                    except:
                        exc_info = sys.exc_info()
                        print(f'Erro de carga do .csv em memória: \n{exc_info}')
                    # TODO: Delete the unzipped .csv file after been processed.
            print(f'Fim da carga dos arquivos CSV para a chave {chave}')
        except:
            print(f'Erro na busca de arquivos zip para a tabela {chave}: \n \t{sys.exc_info()}')
    pass

# Set hook for main execution flow to the ingest_datasets method
if __name__ == "__main__":
    ingest_datasets()
