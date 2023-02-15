import re
import subprocess
import os
import requests
import time
from parsel import Selector
import hashlib
import logging

# Retorna o nome do arquivo a partir da URL completa
def get_file_name(url):
    file_name = None
    if url:
        file_tokens = url.split("/")
        file_name = file_tokens[-1]
    return file_name


# Fluxo principal de execução
def download_datasets():
    # Configurações de logging
    logging.basicConfig(filename="download.log", encoding="utf-8", level=logging.DEBUG)
    downloaded_datasets = []
    datasets_urls = get_datasets_urls()
    n_files = len(list(datasets_urls.copy()))
    logging.info(f"Existem {n_files} arquivos a baixar")
    for url in datasets_urls.copy():
        logging.info(url)
    for url in datasets_urls:
        is_file_downloaded = download_file(url)
        if is_file_downloaded:
            downloaded_datasets.append(url)
    logging.info(f"Foram baixados {len(downloaded_datasets)} arquivos.")


# Método que varre a página html que contém a lista de arquivos .zip com o dump do banco de dados de CNPJs da Receita Federal, retornando uma lista com os resultados
def get_datasets_urls():
    # response = requests.get("https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj")
    response = requests.get("http://200.152.38.155/CNPJ/")
    page = Selector(response.text)
    datasets_urls = page.xpath("//a[contains(@href, '.zip')]/@href").getall()
    # datasets_urls = map(remove_malformed_http, datasets_urls)
    # Transforma o map em lista simples
    # datasets_urls = list(datasets_urls)
    full_urls = []
    for url in datasets_urls:
        full_urls.append("http://200.152.38.155/CNPJ/" + url)
    logging.info(full_urls)
    return full_urls


def remove_malformed_http(url: str):
    return re.sub(r"http//", "", url)


def generate_sha256_checksum(file_name: str, file_size: str):
    encoded_string = (file_name + file_size).encode("utf8")
    file_hash = hashlib.sha256(encoded_string)
    hash_digest_checksum = file_hash.hexdigest()
    return hash_digest_checksum


# Baixa nova versão do arquivo remoto
def get_remote_file(url, file_path, retry: bool = False):
    try:
        # wget parameters
        parameters = f"wget {url} -c -O {file_path}".split()
        # realiza chamada de linha de comando via subprocecss
        subprocess.call(parameters)
        # Verifica se o download deu certo, ou tenta de novo
        retry_download(url, file_path, retry)
        # Verifica se com, ou sem retry, foi possível baixar o arquivos
        return local_file_exists(file_path)
    except Exception as e:
        logging.error("Erro de download:")
        logging.error(e)
        return False


# Função simples para verificar se o arquivo existe ou não em disco
def local_file_exists(file_path):
    # Verificar se uma nova versão do arquivo foi baixada
    return os.path.exists(file_path)


# Recebe o caminho em que o arquivo deveria ter sido salvo e verifica se o mesmo encontra-se no disco
def retry_download(url, file_path, retry: bool = False):
    # Verificar se uma nova versão do arquivo foi baixada
    if not local_file_exists(file_path):
        logging.error(
            f"Não foi possível baixar o arquivo. Tentando download novamente em 2 minutos para o arquivo {file_path}"
        )
        # Se deu erro, esperar 2 minutos e tentar novamente
        if not retry:
            # If it is the first error, Wait 2 minutes and retry file download
            time.sleep(120)
            download_file(url, True)
        else:
            logging.error(
                f"Fim da segunda tentativa. Download do arquivo {file_name} não foi realizado. Tentar novamente em outro momento, ou verificar problema no servidor."
            )


def get_file_path(file_name: str):
    # full path to file
    file_path = "../data/" + file_name
    return file_path


# Se o arquivo já existir e for o mesmo que o arquivo remoto, não baixa e retorna False
# Caso o arquivo local seja diferente do remoto, ou se não existir em disco, baixar arquivo
def download_file(url: str, retry: bool = False):
    # get file name
    file_name = get_file_name(url)
    logging.info(f"Baixando arquivo {file_name}")
    # full path to file
    file_path = get_file_path(file_name)
    # Objeto para armazenar os metadados do arquivo remoto
    info = None
    # Verifica metadados do arquivo remoto para comparação
    try:
        # Chamando primeiro a requisição remota de metadados para verificar o tamanho do arquivo antes de baixar
        info = requests.head(url)
    except Exception as e:
        logging.error(
            f"→ Erro de verificação preventiva de metadados do arquivo {file_name}."
        )
    # Conseguiu a informação sobre o arquivo remoteo
    if info:
        remote_file_size = info.headers["Content-Length"]
        if remote_file_size:
            logging.info(f"Tamanho do arquivo remoto: {remote_file_size} bytes")
            # Caso exista arquivo anterior, calcular checksum e comparar
            logging.info(f"Arquivo a verificar em disco: {file_path}")
            if local_file_exists(file_path):
                local_file_size = str(os.path.getsize(file_path))
                if local_file_size:
                    logging.info(
                        f"Tamanho do arquivo em disco: {remote_file_size} bytes"
                    )
                    # Para garantir a integridade entre cargas, será implementada uma verificação do tamanho do arquivo remoto
                    # contra o arquivo baixado anteriormente, ainda em disco, através do checksum
                    local_file_hash = generate_sha256_checksum(
                        file_name, local_file_size
                    )
                    remote_file_hash = generate_sha256_checksum(
                        file_name, remote_file_size
                    )
                    # Arquivos com tamanhos o nomes diferentes, houve alguma mudança nos arquivos. Substitui o velho pelo novo.
                    if local_file_hash != remote_file_hash:
                        # TODO: Aqui, ao invés de deletar o antigo, vai armazenar o antigo em outro diretório no Spaces e manter o novo
                        logging.info(
                            "O arquivo local em disco e o arquivo remoto tem tamanhos diferentes, ou mudaram de nome. Removendo o arquivo local e baixa do servidor."
                        )
                        # Remove o arquivo antigo
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            logging.error(
                                f"Erro! Não foi possível remover arquivo {file_name}"
                            )
                            logging.error(e)
                        # Realiza a chamada para baixar o novo arquivo
                        return get_remote_file(url, file_path, retry)
                    else:
                        logging.error(
                            "O arquivo já existe em disco e não sofreu alterações, portanto não é necessário baixar novamente."
                        )
                        return False
            else:
                logging.info("Arquivo não existe no disco, é necessário baixar.")
                return get_remote_file(url, file_path, retry)
    # Não foi possível pegar metadados preventivamente, considera não existente e baixa o novo arquivo
    else:
        logging.info(
            f"Não foi possível carregar preventivamente os metadados do arquivo {file_name}. Baixar arquivo."
        )
        return get_remote_file(url, file_path, retry)
    pass


if __name__ == "__main__":
    download_datasets()
