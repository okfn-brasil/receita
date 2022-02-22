# Deve ser executado de dentro do diretório task/
# Para cada tabela/tema (ex.: socio, estabelecimento, cnae, empresa, etc.):
# Descompactar os arquivos .zip da tabela
# Para cada .csv referente à tabela:
# i. Carregar em memória os dados do .csv (EXTRACT)
# ii. Executar qualquer tarefa de limpeza/conversão necessária (TRANSFER)
# iii. Inserir na tabela apropriada do banco de dados qd_receita os registros extraídos dos arquivos .csv (LOAD)

import glob
from zipfile import ZipFile
import pandas
from sqlalchemy import create_engine

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
    # Remove todos os arquivos que não tenham relação com a tabela selecionada para a carga
    for f in zip_files:
        if nome_arquivo not in f:
            zip_files.remove(f)
    # Retorna a lista completa de arquivos .zip da tabela
    return zip_files

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
def carregar_dados(caminho_nome_arquivo: str, tabela: str):
    campos_selecionados = mapeamento_nome_campos[tabela]
    try:
        data_frame = pd.read_csv(caminho_nome_arquivo, names=campos_selecionados)
        if data_frame is not None:
            # Instantiate sqlachemy.create_engine object
            sql_engine = None
            try:
                sql_engine = create_engine('postgresql://postgres:q7Muz4W5iI9@localhost:5432/qd_receita')
            except:
                print(f'Erro de conexão via SQLAlchemy.')
            # Salva no banco de dados os registros:
            if sql_engine is not None:
                try:
                    # iii. Inserir na tabela apropriada do banco de dados qd_receita os registros extraídos dos arquivos .csv (LOAD)
                    data_frame.to_sql(tabela, sql_engine, index=False, if_exists='replace', chunksize=5000)
                    return True
                except:
                    print(f'Erro ao salvar os dados na tabela {tabela}')
    except:
        print(f'Erro de carga do .CSV em memória via pandas')

    return False

# Fluxo de execução principal do programa
def ingest_datasets():
    # Para cada tabela/tema (ex.: socio, estabelecimento, cnae, empresa, etc.):
    for chave in mapeamento_tabelas_zip.keys():
        print(f'Carregando dados para a tabela {chave}')
        try:
            # Pegar lista de nomes de arquivos .zip relacionados à tabela
            lista_zip = pegar_lista_zip(mapeamento_tabelas_zip[chave])
            for arquivo_zip in lista_zip:
                # Descompactar os arquivos .zip da tabela
                descomprimiu = False
                try:
                    descomprimiu = descomprimir_zip(f'../data/{arquivo_zip}')
                except:
                    print(f'Erro de descompressão do arquivo {arquivo_zip}')
                if descomprimiu:
                    # Carregar o csv em memória
                    try:
                        # O arquivo .csv extraído tem o mesmo nome do zip sem a extensão. Adicionando o diretório de dados ao caminho relativo
                        arquivo_csv = '../data/' + arquivo_zip.replace('.zip', '')
                        # Carregar o .csv  em memória e persistir as linhas no banco de dados, usando sqlalchemy
                        sucesso = carregar_dados(arquivo_csv, chave)
                        if sucesso:
                            print(f'Registros carregados com sucesso para o arquivo {arquivo_csv}')
                        else:
                            print(f'Erro na carga do arquivo {arquivo_csv}')
                    except:
                        print(f'Erro de carga do .csv em memória')
        except:
            print(f'Erro na busca de arquivos zip para a tabela {key}')
    pass

# Set hook for main execution flow to the ingest_datasets method
if __name__ == "__main__":
    ingest_datasets()
