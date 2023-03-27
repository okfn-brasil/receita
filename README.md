
## READ.ME

Este repositório busca facilitar o acesso às informações publicadas pela Receita Federal do Brasil sobre o Cadastro Nacional de Pessoa Jurídica, na medida em que fornece uma solução para a criação de uma base de dados relacional local unificada para consultas.

Dessa forma, é possível realizar análises mais profundas com os dados que estão acessíveis através [DESTE SERVIÇO](http://200.152.38.155/CNPJ/). Caso o serviço deixe de estar online ou mude de endereço, o código deverá refletir as alterações. O serviço contém uma listagem com arquivos comprimidos tipo .ZIP que contém, cada um, um arquivo tabular do tipo .CSV (comma-separated-values), representando os dados modelados disponíveis para as seguintes entidades:


1. Empresas
2. Estabelecimentos
3. Sócios
4. CNAEs (Cadastro Nacional de Atividade Empresarial)
5. Simples
6. Municípios
7. Países
8. Natureza (Jurídica)
9. Qualificações (dos Sócios)
10. Motivo (da situação cadastral)

Para cada entidade, pode haver um ou mais arquivos .ZIP correspondentes ao total de informações disponíveis na base, ordenados em sequência.
A relação entre as entidades encontra-se descrita textualmente através do [Arquivo de Layout)](http://200.152.38.155/CNPJ/LAYOUT_DADOS_ABERTOS_CNPJ.pdf) fornecido no próprio endereço e datado de 2018.


## :: Regras de negócio ::

As regras de negócio do Cadastro Nacional de Pessoa Jurídica (CNPJ) podem ser compreendidas de modo simplificado como a representação de um empreendimento do mundo real através de uma entidade do tipo **empresa**, que é única e recebe da Receita Federal um **código de 8 dígitos** conhecido como o **CNPJ base, ou CNPJ básico**.
Além do CNPJ base, a empresa, quando criada, possui apenas o estabelecimento matriz, que recebe um **código de 4 dígitos**, conhecido como **ordem do CNPJ**, que era normalmente sequencial, iniciando em **0001**. Além disso, o estabelecimento recebe um terceiro código de 2 **dígitos conhecido como dígito verificador**, gerado através de um algoritmo da Receita que calcula uma espécie de código hash utilizando a combinação cnpj base e ordem, gerando assim o dígito verificador.
Assim, uma empresa pode possuir múltiplos estabelecimentos, todos compartilhando o mesmo cnpj base, entretanto cada estabelecimento com um código de ordem individual e um dígito verificador correspondente.
O estabelecimento possui informações complementares às informações de empresa, definidos pelas outras entidades a que chamaremos de **dimensões complementares**. Segue abaixo a sugestão adotada para a modelagem das tabelas no projeto:
![Modelo de tabelas do banco](docs/static/image/Entity Relationship Diagram.png)




## :: Fluxo de trabalho ::

Este projeto está dividido portanto em três fases fundamentais, de acordo com o fluxo normal de processos de ETL (Extract, Transform, Load). Neste escopo, o fluxo pode ser definido como:

Baixar os arquivos do serviço online
Carregar os arquivos tabulares como dados brutos estruturando-os em tabelas no banco de dados
Processar os dados disponíveis nas tabelas, realizando validação, formatação e limpeza, produzindo um documento único chamado **resposta_cnpj** que consolida em uma tabela os dados de CNPJ de estabelecimentos e empresas, bem como em outra chamada **resposta_socio** que consolida os dados do quadro de sócios, que é aquilo que está disponibilizado para o usuário final através dos pontos de acesso de estabelecimentos e de sócios da API do Querido Diário.

## :: Instalação ::

Para instalar o projeto, utilize Python 3+. Recomenda-se a criação e ativação de um ambiente virtual python. Basta acessar o diretório do projeto
```
$ cd receita
$ pip3 install -r requirements.txt
```
Caso aconteça algum erro de instalação das dependências, verifique se está utilizando Python3 ou o ambiente virtual correto, se está no diretório raiz do projeto e se houve algum erro em um dos pacotes, tentando abordar o erro pontual que causou o problema de instalação.

## :: Execução ::

Para executar a partir do diretório principal (raiz) do projeto, basta:

Acessar o diretório de tarefas do projeto:
```
$ cd tasks/
```
Acessar o arquivo e alterar as informações de acesso ao banco (usuário, senha, endereço, etc) de acordo com a sua especificação.

Chamar a execução do python via linha de comando:

i) Executa o script que baixa os arquivos .ZIP, caso sejam uma versão mais nova dos dados disponíveis, evitando assim baixar uma versão já disponível em disco. Caso seja uma versão mais nova, a versão anterior em disco é movida para outro diretório e os novos arquivos são baixados.
```
$ python3 download.py
```
ii) Realiza a extração dos arquivos tabulares de dentro dos arquivos .ZIP, carregando os dados  em memória por bloco para cada entidade e persistindo a informação original (raw) em tabelas do banco de dados
	```
		$ python3 ingest.py
	```
iii) Realiza as operações de validação, formatação e limpeza dos dados brutos, salvando um único registro na tabela resposta_cnpj para cada estabelecimento, com todas as informações das tabelas complementares sintetizadas no registro, bem como um registro na tabela resposta_socios para cada sócio de um determinado CNPJ.
```
$ python3 process_cnpjs.py
```
## :: Acesso ::

Os dados da RFB, após processados pela OKBR utilizando este fluxo, podem ser acessados online gratuitamente através dos seguintes pontos de acesso da API pública do Querido Diário:

Endpoint de estabelecimentos
Endpoint de sócios
