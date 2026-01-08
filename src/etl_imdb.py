import os
import tempfile
import requests
import pandas as pd
import sqlite3
import logging

# ============================================
# CONFIGURAÇÃO DE LOGGING
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================
# CONFIGURAÇÃO DE USO TEMPORÁRIO (DISCO D)
# ============================================
os.environ["TMPDIR"] = "D:\\temp"
os.environ["TEMP"] = "D:\\temp"
os.environ["TMP"] = "D:\\temp"
tempfile.tempdir = "D:\\temp"
os.makedirs("D:\\temp", exist_ok=True)

# ============================================
# CONFIGURAÇÕES GERAIS
# ============================================
BASE_URL = "https://datasets.imdbws.com"
ARQUIVOS = [
    "name.basics.tsv.gz",
    "title.basics.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

MAX_ROWS = 300_000  # limite para portfólio
DATA_DIR = "data"
TRATADOS_DIR = os.path.join(DATA_DIR, "tratados")
DB_NAME = "imdb_data.db"

# ============================================
# FUNÇÃO ETL
# ============================================
def execute_etl():

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(TRATADOS_DIR, exist_ok=True)

    # ------------------------
    # EXTRAÇÃO
    # ------------------------
    for arquivo in ARQUIVOS:
        caminho = os.path.join(DATA_DIR, arquivo)
        if not os.path.exists(caminho):
            logging.info(f"Baixando {arquivo}")
            r = requests.get(f"{BASE_URL}/{arquivo}")
            with open(caminho, "wb") as f:
                f.write(r.content)
        else:
            logging.info(f"{arquivo} já existe. Pulando download.")

    # ------------------------
    # TRANSFORMAÇÃO
    # ------------------------
    for arquivo in ARQUIVOS:
        caminho = os.path.join(DATA_DIR, arquivo)
        destino = os.path.join(TRATADOS_DIR, arquivo.replace(".gz", ""))

        logging.info(f"Tratando {arquivo}")

        if "principals" in arquivo:
            df = pd.read_csv(
                caminho,
                sep="\t",
                compression="gzip",
                nrows=MAX_ROWS,
                low_memory=False
            )
        else:
            df = pd.read_csv(
                caminho,
                sep="\t",
                compression="gzip",
                low_memory=False
            )

        df.replace({"\\N": None}, inplace=True)
        df.to_csv(destino, sep="\t", index=False)

    # ------------------------
    # CARGA SQLITE
    # ------------------------
    conn = sqlite3.connect(DB_NAME)

    for arquivo in os.listdir(TRATADOS_DIR):
        caminho = os.path.join(TRATADOS_DIR, arquivo)
        tabela = arquivo.replace(".tsv", "").replace(".", "_")

        logging.info(f"Salvando tabela {tabela}")

        conn.execute(f"DROP TABLE IF EXISTS {tabela}")

        for chunk in pd.read_csv(caminho, sep="\t", chunksize=100_000):
            chunk.to_sql(tabela, conn, if_exists="append", index=False)

    # ------------------------
    # TABELA ANALÍTICA
    # ------------------------
    logging.info("Criando tabela analítica")

    query_analitica = """
    CREATE TABLE IF NOT EXISTS analitico_titulo AS
    SELECT
        tb.tconst,
        tb.titleType,
        tb.originalTitle,
        tb.startYear,
        tb.genres,
        tr.averageRating,
        tr.numVotes,
        COUNT(tp.nconst) AS qtParticipantes
    FROM title_basics tb
    LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
    LEFT JOIN title_principals tp ON tb.tconst = tp.tconst
    GROUP BY tb.tconst;
    """

    conn.execute("DROP TABLE IF EXISTS analitico_titulo")
    conn.execute(query_analitica)
    conn.close()

    logging.info("ETL finalizado com sucesso.")

# ============================================
# EXECUÇÃO
# ============================================
if __name__ == "__main__":
    execute_etl()
