import sqlalchemy
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import select
from sqlalchemy.orm import Session

Base = declarative_base()

### Definição das classes para modelagem da conexão com as tabelas ###
class DimMatrizFilial(Base):
    __tablename__ = "dim_matriz_filial"
    codigo = Column(String(1), primary_key=True)
    descricao = Column(String)


class NaturezaJuridica(Base):
    __tablename__ = "natureza_juridica"
    codigo = Column(String(4), primary_key=True)
    descricao = Column(String)


class DimFaixaEtaria(Base):
    __tablename__ = "dim_faixa_etaria"
    codigo = Column(String(1), primary_key=True)
    descricao = Column(String)


class DimIdentificadorSocio(Base):
    __tablename__ = "dim_identificador_socio"
    codigo = Column(String(1), primary_key=True)
    descricao = Column(String)


class DimPorteEmpresa(Base):
    __tablename__ = "dim_porte_empresa"
    codigo = Column(String(2), primary_key=True)
    descricao = Column(String)


class DimSituacaoCadastral(Base):
    __tablename__ = "dim_situacao_cadastral"
    codigo = Column(String(2), primary_key=True)
    descricao = Column(String)


class Cnae(Base):
    __tablename__ = "cnae"
    codigo = Column(String(7), primary_key=True)
    descricao = Column(String)


class Motivo(Base):
    __tablename__ = "motivo"
    codigo = Column(String(3), primary_key=True)
    descricao = Column(String)


class Municipio(Base):
    __tablename__ = "municipio"
    codigo = Column(String(4), primary_key=True)
    descricao = Column(String)


class Pais(Base):
    __tablename__ = "pais"
    codigo = Column(String(3), primary_key=True)
    descricao = Column(String)


class QualificacaoSocio(Base):
    __tablename__ = "qualificacao_socio"
    codigo = Column(String(3), primary_key=True)
    descricao = Column(String)

class Simples(Base):
    __tablename__ = "simples"
    cnpj_basico = Column(String(8), primary_key=True)
    opcao_pelo_simples = Column(String(1))
    data_opcao_pelo_simples = Column(String(10))
    data_exclusao_pelo_simples = Column(String(10))
    opcao_pelo_mei = Column(String(1))
    data_opcao_pelo_mei = Column(String(10))
    data_exclusao_pelo_mei = Column(String(10))

class PostgresInterface:
    def __init__(self):
        # Declarative Base object
        self.table_objects = {
            "dim_matriz_filial": None,
            "dim_faixa_etaria": None,
            "dim_identificador_socio": None,
            "dim_porte_empresa": None,
            "dim_situacao_cadastral": None,
            "cnae": None,
            "motivo": None,
            "municipio": None,
            "qualificacao_socio": None,
            "pais": None,
        }

        # Engine único de acesso
        self.engine = None
        # Sessão singular para o acesso ao banco
        self.session = None

        # Configurações de logging
        logging.basicConfig(
            handlers=[
                logging.FileHandler(
                    filename="./postgres_interface.log", encoding="utf-8", mode="a+"
                )
            ],
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
            datefmt="%F %A %T",
            level=logging.DEBUG,
        )
        pass

    def get_table_objects(self):
        return self.table_objects

    # Inicializa uma fábrica de acesso ao banco e inicializa o self.table_objects da classe com os todos os registros de todas as tabelas
    def load_all(self):
        session = self.session
        try:
            logging.info("→→→ Carregando tabelas/dimensões auxiliares em memória em objetos tipo dicionário com uma chave para cada código e a descrição como valor do dicionário para a chave")

            matriz_filial = session.execute(select(DimMatrizFilial)).scalars().all()
            matriz_filial_dict = {}
            for item in matriz_filial:
                matriz_filial_dict[item.codigo] = item.descricao
            self.table_objects["dim_matriz_filial"] = matriz_filial_dict

            natureza_juridica = session.execute(select(NaturezaJuridica)).scalars().all()
            natureza_juridica_dict = {}
            for item in natureza_juridica:
                cod = item.codigo
                if cod:
                    natureza_juridica_dict[cod] = item.descricao
            self.table_objects["natureza_juridica"] = natureza_juridica_dict

            faixa_etaria = session.execute(select(DimFaixaEtaria)).scalars().all()
            faixa_etaria_dict = {}
            for item in faixa_etaria:
                cod = item.codigo
                if cod:
                    faixa_etaria_dict[cod] = item.descricao
            self.table_objects["dim_faixa_etaria"] = faixa_etaria_dict

            identificador_socio = session.execute(select(DimIdentificadorSocio)).scalars().all()
            identificador_socio_dict = {}
            for item in identificador_socio:
                cod = item.codigo
                if cod:
                    identificador_socio_dict[cod] = item.descricao
            self.table_objects["dim_identificador_socio"] = identificador_socio_dict

            porte_empresa = session.execute(select(DimPorteEmpresa)).scalars().all()
            porte_empresa_dict = {}
            for item in porte_empresa:
                cod = item.codigo
                if cod:
                    porte_empresa_dict[cod] = item.descricao
            self.table_objects["dim_porte_empresa"] = porte_empresa_dict

            situacao_cadastral = session.execute(select(DimSituacaoCadastral)).scalars().all()
            situacao_cadastral_dict = {}
            for item in situacao_cadastral:
                cod = item.codigo
                if cod:
                    situacao_cadastral_dict[cod] = item.descricao
            self.table_objects["dim_situacao_cadastral"] = situacao_cadastral_dict

            cnae = session.execute(select(Cnae)).scalars().all()
            cnae_dict = {}
            for item in cnae:
                cod = item.codigo
                if cod:
                    cnae_dict[cod] = item.descricao
            self.table_objects["cnae"] = cnae_dict

            motivo = session.execute(select(Motivo)).scalars().all()
            motivo_dict = {}
            for item in motivo:
                cod = item.codigo
                if cod:
                    motivo_dict[cod] = item.descricao
            self.table_objects["motivo"] = motivo_dict

            municipio = session.execute(select(Municipio)).scalars().all()
            municipio_dict = {}
            for item in municipio:
                cod = item.codigo
                if cod:
                    municipio_dict[cod] = item.descricao
            self.table_objects["municipio"] = municipio_dict

            qualificacao_socio = session.execute(select(QualificacaoSocio)).scalars().all()
            qualificacao_socio_dict = {}
            for item in qualificacao_socio:
                cod = item.codigo
                if cod:
                    qualificacao_socio_dict[cod] = item.descricao
            self.table_objects["qualificacao_socio"] = qualificacao_socio_dict

            pais = session.execute(select(Pais)).scalars().all()
            pais_dict = {}
            for item in pais:
                cod = item.codigo
                if cod:
                    pais_dict[cod] = item.descricao
            self.table_objects["pais"] = pais_dict

        except Exception as e:
            logging.error("Erro de carga dos objetos em memória")
            logging.error(e)
        pass

    # Método para acessar dados da tabela "simples" via SQLAlchemy
    def get_simples(self, cnpj_basico:str):
        try:
            # Statement que retorna todos os elementos da tabela que tiverem esse cnpj_basico
            stmt = select(Simples).where(Simples.cnpj_basico == cnpj_basico)
            # Eexecuta a chamada ao banco com o arquivo de sessão
            simples = self.session.execute(stmt)
            if simples:
                simples = simples.scalars().all()
                if simples:
                    # Basta retornar apenas o primeiro da lista
                    return simples[0]
            else:
                return None
        except Exception as e:
            logging.error("Erro de carga dos objetos em memória")
            logging.error(e)


    # Main flow
    def run(self):

        try:
            # Define o endereço para conexão com o banco para o motor do sqlalchemy
            engine = create_engine(
                "postgresql://postgres:postgres@127.0.0.1/qd_receita"
            )

            if engine:
                self.engine = engine
                # Inicializa uma sessão única para acesso global ao banco
                session = Session(engine, future=True)
                if session:
                    self.session = session
                    # Conexao com o banco realizada com sucesso
                    logging.info("Conexão com o banco realizada com sucesso")
                # Realiza a chamada para carregar os dados do banco em memória
                self.load_all()
            else:
                logging.error("Erro de criação do engine SQLAlchemy")
        except Exception as e:
            logging.error("Erro de conexão do SQLAlchemy com o banco PostgreSQL")
            logging.error(e)

    pass  # end of class definition
