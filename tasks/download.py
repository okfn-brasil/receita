import re
import subprocess

import requests
from parsel import Selector


def download_datasets():
    datasets_urls = get_datasets_urls()
    for url in datasets_urls:
        result = download_file(url)
        print('Finished:', result)

    return datasets_urls


def get_datasets_urls():
    response = requests.get("https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj")
    page = Selector(response.text)
    datasets_urls = page.xpath("//a[contains(@href, '.zip')]/@href").getall()
    datasets_urls = map(remove_malformed_http, datasets_urls)
    return datasets_urls


def remove_malformed_http(url: str):
    return re.sub(r"http//", "", url)


def download_file(url: str):
    print(f"Downloading: {url}")
    subprocess.call(f'aria2c --auto-file-renaming=false --continue=true -x 5 --dir=data {url}'.split())
    return url


if __name__ == "__main__":
    download_datasets()

