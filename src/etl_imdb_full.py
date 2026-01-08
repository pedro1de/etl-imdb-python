import os
import tempfile
import requests
import pandas as pd
import sqlite3
import logging
import schedule
import time

# ============================================
# CONFIGURAÇÃO DE LOGGING
# ============================================
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


# ============================================
# CONFIGURAÇÃO DE USO TEMPORÁRIO NO DISCO D
# ============================================
# Este bloco força o Python, Pandas e SQLite a usarem o disco D
# para criar arquivos temporários, evitando o erro "database or disk is full"
# que ocorre quando o disco C está com pouco espaço.

os.environ["TMPDIR"] = "D:\\temp"
os.environ["TEMP"] = "D:\\temp"
os.environ["TMP"] = "D:\\temp"
tempfile.tempdir = "D:\\temp"
os.makedirs("D:\\temp", exist_ok=True)
# ============================================


# ============================================
# FUNÇÃO PRINCIPAL DE ETL
# ============================================
def execute_script():
    # ========================
    # DOWNLOAD DOS ARQUIVOS
    # ========================
    base_url = "https://datasets.imdbws.com"

    arquivos = [
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"
    ]

    destino_diretorio = "data"
    os.makedirs(destino_diretorio, exist_ok=True)

    for arquivo in arquivos:
        url = base_url + "/" + arquivo
        caminho_destino = os.path.join(destino_diretorio, arquivo)

        if not os.path.exists(caminho_destino):
            logging.debug(f"Baixando {arquivo}...")
            response = requests.get(url)
            if response.status_code == 200:
                with open(caminho_destino, 'wb') as f:
                    f.write(response.content)
                    logging.debug(f"{arquivo} baixado com sucesso")
            else:
                logging.error(f"Falha ao baixar {arquivo}. Código de status: {response.status_code}")
        else:
            logging.info(f"{arquivo} já existe. Pulando download.")

    logging.info("Download Concluído.")

    # ========================
    # TRATAMENTO DOS DADOS
    # ========================
    diretorio_dados = "data"
    diretorio_tratados = os.path.join(diretorio_dados, "tratados")
    os.makedirs(diretorio_tratados, exist_ok=True)

    arquivos = os.listdir(diretorio_dados)

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio_dados, arquivo)

        if os.path.isfile(caminho_arquivo) and arquivo.endswith("gz"):
            logging.info(f"Lendo e tratando arquivo {arquivo}...")

            df = pd.read_csv(caminho_arquivo, sep='\t', compression='gzip', low_memory=False)
            df.replace({"\\N": None}, inplace=True)

            caminho_destino = os.path.join(diretorio_tratados, arquivo[:-3])
            df.to_csv(caminho_destino, sep='\t', index=False)

            logging.debug(f"Tratamento concluído para {arquivo}. Arquivo tratado salvo em {caminho_destino}")

    logging.info("Todos os arquivos foram tratados e salvos no diretório 'tratados'.")

    # ========================
    # SALVANDO EM BANCO DE DADOS SQLITE
    # ========================
    diretorio_tratados = os.path.join("data", "tratados")
    banco_dados = "imdb_data.db"

    conexao = sqlite3.connect(banco_dados)
    arquivos = os.listdir(diretorio_tratados)
    chunksize = 100_000

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio_tratados, arquivo)

        if os.path.isfile(caminho_arquivo) and arquivo.endswith(".tsv"):
            logging.debug(f"Processando {arquivo}...")

            nome_tabela = os.path.splitext(arquivo)[0]
            nome_tabela = nome_tabela.replace(".", "_").replace("-", "_")

            conexao.execute(f"DROP TABLE IF EXISTS {nome_tabela}")

            for chunk in pd.read_csv(
                caminho_arquivo,
                sep="\t",
                chunksize=chunksize,
                low_memory=False
            ):
                chunk.to_sql(
                    nome_tabela,
                    conexao,
                    index=False,
                    if_exists="append"
                )

            logging.debug(f"Tabela {nome_tabela} salva com sucesso.")

    conexao.close()
    logging.info("Todos os arquivos foram salvos no banco de dados.")

    # ========================
    # CRIAÇÃO DAS TABELAS ANALÍTICAS
    # ========================
    analitico_titulo = """
    CREATE TABLE IF NOT EXISTS analitico_titulo AS
    WITH participantes AS (
        SELECT 
            tb.tconst,
            tb.titleType,
            tb.originalTitle,
            tb.endYear,
            tb.genres,
            tr.averageRating,
            tr.numVotes,
            tp.qtParticipantes
        FROM title_basics tb
        LEFT JOIN title_ratings tr
            ON tr.tconst = tb.tconst
        LEFT JOIN (
            SELECT 
                tconst,
                COUNT(DISTINCT nconst) AS qtParticipantes
            FROM title_principals
            GROUP BY tconst
        ) AS tp
            ON tp.tconst = tb.tconst
    )
    SELECT * FROM participantes;
    """

    analitico_participante = """
    SELECT
        tp.nconst,
        tp.tconst,
        tp.ordering,
        tp.category,
        tb.genres
    FROM title_principals tp
    LEFT JOIN title_basics tb
        ON tb.tconst = tp.tconst;
    """

    queries = [analitico_titulo, analitico_participante]

    for query in queries:
        banco_dados = "imdb_data.db"
        conexao = sqlite3.connect(banco_dados)
        conexao.execute(query)
        conexao.close()

    logging.info("Tabelas criadas com sucesso.")



# ============================================
# AGENDAMENTO DIÁRIO
# ============================================
schedule.every().day.at("02:00").do(execute_script)

while True:
    schedule.run_pending()
    time.sleep(1)  # Espera um segundo entre as verificações
