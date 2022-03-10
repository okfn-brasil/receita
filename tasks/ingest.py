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

# i. Carregar em memória os dados do .csv (EXTRACT) usando o pandas e sqlalchemy
# A referência ao arquivo .csv é a string caminho_nome_arquivo, tabela é a chave da tabela em que será inserida
# Retorna True caso a operação realize o registro dos dados no banco e False caso contrário
def carregar_dados(caminho_nome_arquivo: str, tabela: str, sql_engine):
    campos_selecionados = mapeamento_nome_campos[tabela]
    try:
        data_frame = pd.read_csv(caminho_nome_arquivo, delimiter=';', names=campos_selecionados, encoding='latin-1', on_bad_lines='warn', chunksize=5000)
        if data_frame is not None:
            # Salva no banco de dados os registros:
            if sql_engine is not None:
                try:
                    # iii. Inserir na tabela apropriada do banco de dados qd_receita os registros extraídos dos arquivos .csv (LOAD)
                    total_records_updated = data_frame.to_sql(tabela, sql_engine, index=False, if_exists='replace', chunksize=5000, method='multi')
                    print(f'{total_records_updated} registros salvos na tabela {tabela}')
                    return True
                except Exception as e:
                    print(f'Erro {e} ao salvar os dados na tabela {tabela}')
    except:
        exc_info = sys.exc_info()
        print(f'Erro de carga do .CSV em memória via pandas: {exc_info}')

    return False

def carregar_tabela(caminho_arquivo, nome_tabela, nome_db, host, porta, user, pwd):
    '''
    This function uploads csv to a target table
    '''
    conn, cur = None, None
    try:
        conn = psycopg2.connect(dbname=nome_db, host=host, port=porta, user=user, password=pwd)
        conn.autocommit = True
        print("Conectado ao BD com sucesso.")
    except Exception as e:
        print("Erro de conexão ao banco de dados: {}".format(str(e)))
        sys.exit(1)
    if conn is not None:
        f = None
        try:
            f = open(caminho_arquivo, "r")
            print(f'Abriu o arquivo: {caminho_arquivo}')
        except Exception as e:
            print("Erro de abertura do arquivo: {}".format(str(e)))
            sys.exit(1)
        if f is not None:
            cur = conn.cursor()
            # Truncate the table first
            cur.execute("Truncate {} Cascade;".format(nome_tabela))
            print(f'Truncated {nome_tabela}')
            # Load table from the file without HEADER
            cur.copy_expert("copy {} from STDIN CSV QUOTE '\"'".format(nome_tabela), f)
            cur.execute("commit;")
            print("Loaded data into {}".format(nome_tabela))
            conn.close()
            print("Conexão com o banco fechada.")

# Fluxo de execução principal do programa
def ingest_datasets():
    # Instantiate sqlachemy.create_engine object
    sql_engine = None
    try:
        sql_engine = create_engine('postgresql://postgres:q7Muz4W5iI9@localhost:5432/qd_receita')
        print(f'Sql engine loaded {sql_engine}')

    except:
        print(f'Erro de conexão via SQLAlchemy: {sys.exc_info()}')

    # Para cada tabela/tema (ex.: socio, estabelecimento, cnae, empresa, etc.):
    for chave in sorted(mapeamento_tabelas_zip.keys(), key=str.lower):
        print(f'Carregando dados para a tabela {chave}')
        try:
            # Pegar lista de nomes de arquivos .zip relacionados à tabela
            lista_zip = pegar_lista_zip(mapeamento_tabelas_zip[chave])
            lista_zip = sorted(lista_zip, key=str.lower, reverse=True)
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
                        # Carregar o .csv  em memória e persistir as linhas no banco de dados, usando sqlalchemy
                        sucesso = carregar_dados(arquivo_csv, chave, sql_engine)
                        # sucesso = carregar_tabela('arquivo_csv', chave, 'qd_receita', 'localhost', '5432', postgres, 'pwd)
                        print(f'A carga dos dados demorou {(time.time() - start_time)} segundos.')
                        if sucesso:
                            print(f'Registros carregados com sucesso para o arquivo {arquivo_csv}')
                        else:
                            print(f'Erro na carga do arquivo {arquivo_csv}')
                    except:
                        exc_info = sys.exc_info()
                        print(f'Erro de carga do .csv em memória: {exc_info}')
            print(f'Fim da carga dos arquivos CSV para a chave {chave}')
        except:
            print(f'Erro na busca de arquivos zip para a tabela {chave}: \n \t{sys.exc_info()}')
    pass

# Set hook for main execution flow to the ingest_datasets method
if __name__ == "__main__":
    ingest_datasets()
