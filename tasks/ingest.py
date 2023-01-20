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

# Mapeamento entre os nomes das tabelas no banco e uma referência ao nome do arquivo em disco para os arquivos .zip
mapeamento_tabelas_zip = {
    "empresa": "Empresas",  # "Empresas",
    "estabelecimento": "Estabelecimentos",  # "Estabelecimentos",
    "simples": "Simples",
    "socio": "Socios",
    "pais": "Paises",  # "Paises",
    "municipio": "Municipios",  # "Municipios",
    "qualificacao_socio": "Qualificacoes",
    "natureza_juridica": "Naturezas",  # "Naturezas",
    "cnae": "Cnaes",  # "Cnaes",
    "motivo": "Motivos",
}

# Mapeamento entre os nomes das tabelas no banco e uma referência ao nome do arquivo em disco para os arquivos .csv
mapeamento_tabelas_csv = {
    "empresa": "EMPRECSV",  # "Empresas",
    "estabelecimento": "ESTABELE",  # "Estabelecimentos",
    "simples": "SIMPLES",
    "socio": "SOCIOCSV",
    "pais": "Paises",  # "Paises",
    "municipio": "Municipios",  # "Municipios",
    "qualificacao_socio": "Qualificacoes",
    "natureza_juridica": "Naturezas",  # "Naturezas",
    "cnae": "Cnaes",  # "Cnaes",
    "motivo": "Motivos",
}

# Mapeamento campos por [índice] .csv estabelecimento → nome dos campos tabela BD
mapeamento_nome_campos = {
    "empresa": {
        "cnpj": "str",
        "razao_social": "str",
        "codigo_natureza_juridica": "str",
        "qualificacao_do_responsavel": "str",
        "capital_social": "str",
        "porte": "str",
        "ente_federativo_responsavel": "str",
    },
    "estabelecimento": {
        "cnpj_basico": "str",
        "cnpj_ordem": "str",
        "cnpj_dv": "str",
        "identificador_matriz_filial": "str",
        "nome_fantasia": "str",
        "situacao_cadastral": "str",
        "data_situacao_cadastral": "str",
        "motivo_situacao_cadastral": "str",
        "nome_cidade_exterior": "str",
        "pais": "str",
        "data_inicio_atividade": "str",
        "cnae_fiscal": "str",
        "cnae_fiscal_secundario": "str",
        "tipo_logradouro": "str",
        "logradouro": "str",
        "numero": "str",
        "complemento": "str",
        "bairro": "str",
        "cep": "str",
        "uf": "str",
        "municipio": "str",
        "ddd_1": "str",
        "telefone_1": "str",
        "ddd_2": "str",
        "telefone_2": "str",
        "ddd_fax": "str",
        "telefone_fax": "str",
        "correio_eletronico": "str",
        "situacao_especial": "str",
        "data_situacao_especial": "str",
    },
    "simples": {
        "cnpj_basico": "str",
        "opcao_pelo_simples": "str",
        "data_opcao_pelo_simples": "str",
        "data_exclusao_pelo_simples": "str",
        "opcao_pelo_mei": "str",
        "data_opcao_pelo_mei": "str",
        "data_exclusao_pelo_mei": "str",
    },
    "socio": {
        "cnpj_basico": "str",
        "identificador_socio": "str",
        "razao_social": "str",
        "cnpj_cpf_socio": "str",
        "codigo_qualificacao_socio": "str",
        "data_entrada_sociedade": "str",
        "codigo_pais_socio_estrangeiro": "str",
        "numero_cpf_representante_legal": "str",
        "nome_representante_legal": "str",
        "codigo_qualificacao_representante_legal": "str",
        "faixa_etaria": "str",
    },
    "pais": {"codigo": "str", "descricao": "str"},
    "municipio": {"codigo": "str", "descricao": "str"},
    "qualificacao_socio": {"codigo": "str", "descricao": "str"},
    "natureza_juridica": {"codigo": "str", "descricao": "str"},
    "cnae": {"codigo": "str", "descricao": "str"},
}

# Dicionário utilizado para armazenar a contagem de registros a carregar e os carregados
n_registros_carregados = {
    "empresa": {"n_items_csv": None, "n_carregados": None},
    "estabelecimento": {"n_items_csv": None, "n_carregados": None},
    "simples": {"n_items_csv": None, "n_carregados": None},
    "socio": {"n_items_csv": None, "n_carregados": None},
    "pais": {"n_items_csv": None, "n_carregados": None},
    "municipio": {"n_items_csv": None, "n_carregados": None},
    "qualificacao_socio": {"n_items_csv": None, "n_carregados": None},
    "natureza_juridica": {"n_items_csv": None, "n_carregados": None},
    "cnae": {"n_items_csv": None, "n_carregados": None},
}

# Pegar a lista de todos os nomes de arquivo .zip que tenham uma referência à string "nome_arquivo"
def pegar_lista_zip(nome_arquivo: str):
    zip_files = glob.glob(r"../data/*.zip")
    items_tabela = []
    # Remove todos os arquivos que não tenham relação com a tabela selecionada para a carga
    for f in zip_files:
        if nome_arquivo in f:
            items_tabela.append(f)
    # Retorna a lista completa de arquivos .zip da tabela
    return items_tabela

# Recebe o nome do arquivo que deve ser um CSV e verifica extensão, presença de caracteres de escape e os remove, retornando uma string limpa sugerida para o arquivo .csv extraído do .zip
def limpa_nome_csv(nome_arquivo: str):
    if  "" in nome_arquivo:
        nome_arquivo = nome_arquivo.replace(" ", "")
    if  "$" in nome_arquivo:
        nome_arquivo = nome_arquivo.replace("$", "")
    if  "\'" in nome_arquivo:
        nome_arquivo = nome_arquivo.replace("\'", "")
    if ".csv" not in nome_arquivo:
        nome_arquivo = nome_arquivo + ".csv"
    return nome_arquivo

# Descompactar um arquivo .zip da tabela a partir da string com referência completa ao diretório e nome do arquivo
# Retorna True caso realize a descompressão com sucesso e False, caso contrário/
def descomprimir_zip(caminho_nome_zip: str):
    # Diretório onde estarão armazenados os dados .CSV após extraídos do .zip
    dir_zip ="../data/"
    # Diretório onde estarão armazenados os dados .CSV após extraídos do .zip
    dir_csv ="../data/extracted/"
    # Lista de arquivos extraídos de dentro do .zip
    csv_extraidos = []
    try:
        print(f"Abrindo arquivo {caminho_nome_zip}:")
        with ZipFile(caminho_nome_zip, "r") as arquivo_zip:
            # Pega a lista de arquivos para verificar o nome do arquivo CSV
            lista_arquivos = arquivo_zip.namelist()
            for arquivo in lista_arquivos:
                # Limpa e define o nome do arquivo .csv:
                target_name = limpa_nome_csv(arquivo)
                # Define o caminho completo para o arquivo após extraído
                target_path = os.path.join(dir_csv, target_name)
                # Criando arquivo .csv em disco para receber o que for extraído do .zip
                with open(target_path, "wb") as f:  # open the output path for writing
                    try:
                        # carrega os bytes do arquivo em memória:
                        arquivo_extraido_bytes = arquivo_zip.read(arquivo)
                        try:
                             # salva os bytes no arquivo .csv no caminho especificado
                            f.write(arquivo_extraido_bytes)
                            # Aqui adiciona o caminho completo pro arquivo .csv extraído que será novamente carregado em memória na próxima etapa usando Pandas
                            csv_extraidos.append(target_path)
                        except Exception as e:
                            print("Erro de escrita do arquivo .csv")
                            print(e)
                    except Exception as g:
                        print("Erro de extração do arquivo CSV do .zip")
                        print(g)

                print(f"Arquivo {arquivo} sendo extraído para {dir_csv}")
                # Extrair no mesmo diretório data
                # arquivo.extractall(path="../data/")

                # TODO: talvez tenha que pegar o arquivo aqui pelo nome inicial e mudar o nome após extraído na pasta, com sistema de arquivos.
            print(f"Arquivo {arquivo} extraído para {dir_csv} com suesso")
            return csv_extraidos
    except:
        print(f"Erro de descompressão do arquivo {caminho_nome_zip}")
    return None


# Realiza uma regra de três para calcular a porcentagem de andamento da carga dos dados
def status_carga(parcial: int, total: int):
    if total != 0:
        x = (parcial * 100) / total
        # Arrendondando os valores
        x = round(x)
        return str(x)
    return None


# i. Carregar em memória os dados do .csv (EXTRACT) usando o pandas e sqlalchemy
# A referência ao arquivo .csv é a string caminho_nome_arquivo, tabela é a chave da tabela em que será inserida
# Retorna True caso a operação realize o registro dos dados no banco e False caso contrário
def carregar_dados(caminho_nome_arquivo: str, tabela: str, sql_engine):
    # Como poderiam vir vários arquivos dentro do .zip, recebemos uma lista, mas só nos interessa o primeiro (e único elemento) da lista
    caminho_nome_arquivo = caminho_nome_arquivo[0]
    print(f"Caminho do arquivo .csv a carregar em memória: {caminho_nome_arquivo}")
    # Campos selecionados do csv (?)
    campos_selecionados = list(mapeamento_nome_campos[tabela].keys())
    tipos_selecionados = mapeamento_nome_campos[tabela]
    # resultado é o objeto a ser retornado com as contagens de registros carregar_dados
    resultado = {}
    try:
        # Valores padrão estão configurados diretamente aqui
        read_chunk_size = 100_000
        write_chunk_size = 10_000
        # Total de items a serem carregados do arquivo csv
        n_items_csv = 0
        # Total de items já salvos no banco de dados
        n_carregados = 0

        # Configurando datatypes para evitar erros de carga do .CSV para a memória via pandas
        data_types = {}

        # Passo intermediário para pegar o número total de items do .csv
        with pd.read_csv(
            caminho_nome_arquivo,
            chunksize=read_chunk_size,
            delimiter=";",
            names=campos_selecionados,
            encoding="latin-1",
            on_bad_lines="warn",
            header=None,
            index_col=False,
            low_memory=False,
        ) as csv_reader:
            for chunk in csv_reader:
                n_items_csv = n_items_csv + int(chunk.shape[0])
            print(f"Total de {n_items_csv} a serem carregados em memória")
        with pd.read_csv(
            caminho_nome_arquivo,
            dtype=tipos_selecionados,
            chunksize=read_chunk_size,
            delimiter=";",
            names=campos_selecionados,
            encoding="latin-1",
            on_bad_lines="warn",
            header=None,
            index_col=False,
            low_memory=False,
        ) as csv_reader:
            print(f"Salvando os registros na tabela {tabela}")
            for chunk in csv_reader:
                # Salva no banco de dados os registros:
                if sql_engine is not None:
                    try:
                        n_in_chunk = len(chunk)
                        # iii. Inserir na tabela apropriada do banco de dados qd_receita os registros extraídos dos arquivos .csv (LOAD)
                        start_insert = time.time()
                        # Salva os dados da fatia em registros no banco de dados
                        chunk.to_sql(
                            tabela,
                            sql_engine,
                            index=False,
                            if_exists="append",
                            chunksize=write_chunk_size,
                            method="multi",
                        )
                        # Atualiza o número de registros carregados até o momento
                        n_carregados = n_carregados + n_in_chunk
                        # Atualiza a porcentagem atual da carga
                        porcentagem = status_carga(n_carregados, n_items_csv)
                        end_insert = time.time() - start_insert
                        if end_insert < 60:
                            end_insert = round(end_insert)
                            print(
                                f"*{tabela}* Foram salvos {n_carregados} registros de um total de {n_items_csv} ({porcentagem}%) [{end_insert} segundos]."
                            )
                        else:
                            minutes = round(end_insert / 60)
                            print(
                                f"*{tabela}* Foram salvos {n_carregados} registros de um total de {n_items_csv} ({porcentagem}%) [{minutes} minutos]."
                            )
                    except Exception as e:
                        print(f"Erro ao salvar os dados na tabela {tabela}:")
                        print(e)

            # soma os valores se já existe algum valor não nulo. Quando está nulo, adiciona o novo valor.
            current_loaded = n_registros_carregados[tabela]["n_items_csv"]
            # Se o valor já existe, soma
            if current_loaded:
                print(f"current_loaded: {current_loaded}")
                n_registros_carregados[tabela]["n_items_csv"] = (
                    current_loaded + n_items_csv
                )
                tabela_n_items_csv = n_registros_carregados[tabela]["n_items_csv"]
                print(f"n_registros_carregados[{tabela}]: {tabela_n_items_csv}")
            else:
                n_registros_carregados[tabela]["n_items_csv"] = n_items_csv
            tabela_n_items_csv = n_registros_carregados[tabela]["n_items_csv"]
            print(
                f'n_registros_carregados[{tabela}]["n_items_csv"] = {tabela_n_items_csv}'
            )

            # soma os valores se já existe algum valor não nulo. Quando está nulo, adiciona o novo valor.
            current_loaded = n_registros_carregados[tabela]["n_carregados"]
            # Se o valor já existe, soma
            if current_loaded:
                n_registros_carregados[tabela]["n_carregados"] = (
                    current_loaded + n_carregados
                )
            else:
                n_registros_carregados[tabela]["n_carregados"] = n_carregados
            tabela_n_registros_carregados = n_registros_carregados[tabela][
                "n_carregados"
            ]
            print(
                f'n_registros_carregados[{tabela}]["n_carregados"] = {tabela_n_registros_carregados}'
            )
            # Deletando o arquivo CSV após a carga.
            try:
                os.remove(caminho_nome_arquivo)
                print(f"→ Arquivo [{caminho_nome_arquivo}] deletado com sucesso.")
            except Exception as e:
                print(f"→ Erro ao remover arquivo [{caminho_nome_arquivo}].")
                print(f"→ Ocorreu algo inesperado: [{str(e)}].")
    except:
        exc_info = sys.exc_info()
        print(f"Erro de carga do .CSV em memória via pandas: {exc_info}")


# Fluxo de execução principal do programa
def ingest_datasets():
    # Instantiate sqlachemy.create_engine object
    sql_engine = None
    try:
        # Remember to set here
        db_password = "change here"
        # Adding pass to the connection string
        db_url = f"postgresql://postgres:{db_password}@localhost:5432/qd_receita"
        sql_engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
        print(f"Motor de acesso ao SGBD PostgreSQL conectado com sucesso: {sql_engine}")
    except:
        print(f"Erro de conexão via SQLAlchemy: {sys.exc_info()}")

    # Para cada tabela/tema (ex.: socio, estabelecimento, cnae, empresa, etc.):
    # for chave in sorted(mapeamento_tabelas_zip.keys(), key=str.lower, reverse=False):
    for chave in mapeamento_tabelas_zip.keys():
        print(
            f"*{chave}* Carregando dados do CSV em memória e processando para a tabela no banco de dados..."
        )
        try:
            # Pegar lista de nomes de arquivos .zip relacionados à tabela
            lista_zip = pegar_lista_zip(mapeamento_tabelas_zip[chave])
            lista_zip = sorted(lista_zip, key=str.lower, reverse=False)
            print(f"Lista de arquivos para a tabela: \n {lista_zip}")
            n_linhas_total = 0
            for arquivo_zip in lista_zip:
                # Descompactar os arquivos .zip da tabela
                descomprimiu = False
                try:
                    print(f"Descomprinindo arquivo {arquivo_zip}")
                    arquivo_csv = descomprimir_zip(f"{arquivo_zip}")
                    descomprimiu = True
                except Exception as e:
                    print(f"Erro de descompressão do arquivo {arquivo_zip}")
                    print(e)
                if descomprimiu:
                    # Carregar o csv em memória
                    try:
                        start_time = time.time()
                        print(
                            f"Arquivo {arquivo_zip} descomprimido com sucesso. Carregando CSV em memória para salvar registros na tabela do banco de dados..."
                        )
                        # Implementação multi-thread
                        t1 = time.time()
                        with ThreadPoolExecutor(max_workers=3) as processPool:
                            future_result = processPool.submit(
                                carregar_dados, arquivo_csv, chave, sql_engine
                            )
                            # future_result = processPool.submit(carregar_tabela, arquivo_csv, chave, 'qd_receita', 'localhost', '5432', 'postgres', change_password)
                            sucesso = future_result.result()
                            # TODO: Verificar se o resultado corresponde a um objeto com o número de linhas no arquivo e o total de registros
                            # print(f'Resultado para a chave {chave}: \n{sucesso}')
                            t2 = time.time()
                            tt = t2 - t1
                            if tt > 60:
                                tt = round(tt / 60)
                                print(f"Demorou {tt} minutos")
                            else:
                                tt = str(tt)[:-3]
                                print(f"Demorou {tt} segundos")
                        # Carregar o .csv  em memória e persistir as linhas no banco de dados, usando sqlalchemy
                        # sucesso = carregar_dados(arquivo_csv, chave, sql_engine)
                        # sucesso = carregar_tabela(arquivo_csv, chave, 'qd_receita', 'localhost', '5432', 'postgres', pwd)
                        tempo_carga = time.time() - start_time
                        if tempo_carga > 60:
                            tempo_carga = round(tempo_carga / 60)
                            print(f"A carga dos dados demorou {tempo_carga} minutos.")
                        else:
                            tempo_carga = str(tempo_carga)[:-3]
                            print(f"A carga dos dados demorou {tempo_carga} segundos.")
                    except:
                        exc_info = sys.exc_info()
                        print(f"Erro de carga do .csv em memória: \n{exc_info}")
                    # TODO: Delete the unzipped .csv file after been processed.
            print(f"Fim da carga dos arquivos CSV para a chave {chave}")
        except:
            print(
                f"Erro na busca de arquivos zip para a tabela {chave}: \n \t{sys.exc_info()}"
            )
    print("→→→ Contagem de registros baixados e inseridos:")
    print(n_registros_carregados)
    empresa_n_items_csv = n_registros_carregados["empresa"]["n_items_csv"]
    empresa_n_carregados = n_registros_carregados["empresa"]["n_carregados"]
    estabelecimento_n_items_csv = n_registros_carregados["estabelecimento"][
        "n_items_csv"
    ]
    estabelecimento_n_carregados = n_registros_carregados["estabelecimento"][
        "n_carregados"
    ]
    print("EMPRESA")
    print(f"CSV: {empresa_n_items_csv} | Registros: {empresa_n_carregados}")
    print("ESTABELECIMENTO")
    print(
        f"CSV: {estabelecimento_n_items_csv} | Registros: {estabelecimento_n_carregados}"
    )
    if empresa_n_items_csv != estabelecimento_n_items_csv:
        porcentagem = status_carga(estabelecimento_n_items_csv, empresa_n_items_csv)
        print(
            f"*** Itens esperados:\tEstabalecimentos: {estabelecimento_n_items_csv} \t| Empresa: {empresa_n_items_csv} ({porcentagem}%)"
        )
    else:
        print(
            "Mesmo número de linhas a serem carregadas para estabelecimentos e empresas!"
        )
    if empresa_n_carregados != estabelecimento_n_carregados:
        porcentagem = status_carga(estabelecimento_n_carregados, empresa_n_carregados)
        print(
            f"*** Itens carregados:\tEstabalecimentos: {estabelecimento_n_items_csv} \t| Empresa: {empresa_n_items_csv} ({porcentagem}%)"
        )
    else:
        print("Mesmo número de linhas carregadas para estabelecimentos e empresas!")
    pass


# Set hook for main execution flow to the ingest_datasets method
if __name__ == "__main__":
    ingest_datasets()
