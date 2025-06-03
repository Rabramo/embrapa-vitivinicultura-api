import os
import json
import logging
import requests
import sqlite3
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from prophet import Prophet
import pickle

from passlib.context import CryptContext

# ===============================
# CONFIGURAÇÕES GERAIS
# ===============================
BASE_URL = "http://vitibrasil.cnpuv.embrapa.br/download/"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
PASTA_RAW = os.path.join(DATA_DIR, "raw")
PASTA_PROC = os.path.join(DATA_DIR, "processed")
DB_PATH = os.path.join(PASTA_PROC, "embrapa.db")
ARQUIVO_STATE = os.path.join(os.path.dirname(__file__), "state.json")
MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")
FORECAST_MODEL_PATH = os.path.join(MODELS_DIR, "forecast_producao_rs.pkl")
FORMATO_DATA = "%Y-%m-%d %H:%M"

# ===============================
# CONFIGURAÇÃO DE LOG
# ===============================
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "update.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ===============================
# CONTEXTO PASSLIB PARA SENHA
# ===============================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def criar_tabela_usuarios():
    """
    Cria a tabela 'users' em embrapa.db se não existir,
    e insere um usuário padrão 'admin' com senha 'admin123'.
    """
    os.makedirs(PASTA_PROC, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        hashed_password TEXT NOT NULL
    );
    """)
    cursor.execute("SELECT username FROM users WHERE username = 'admin';")
    if cursor.fetchone() is None:
        hashed = pwd_context.hash("admin123")
        cursor.execute(
            "INSERT INTO users (username, hashed_password) VALUES (?, ?);",
            ("admin", hashed)
        )
        logging.info("Usuário padrão 'admin' criado com senha 'admin123'.")
    conn.commit()
    conn.close()

def carregar_estado_local() -> dict:
    if os.path.exists(ARQUIVO_STATE):
        with open(ARQUIVO_STATE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def salvar_estado_local(estado: dict):
    with open(ARQUIVO_STATE, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)

def parse_datetime(texto: str) -> datetime:
    return datetime.strptime(texto, FORMATO_DATA)

def obter_lista_remota() -> dict:
    """
    Lê a página /download/ do site e retorna { nome_arquivo.csv: last_modified_str }.
    """
    resp = requests.get(BASE_URL, timeout=15)
    resp.raise_for_status()
    resp.encoding = "latin-1"
    soup = BeautifulSoup(resp.text, "html.parser")

    tabela = soup.find("table")
    if tabela is None:
        raise RuntimeError("Tabela de index não encontrada em /download/")

    lista = {}
    for tr in tabela.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 3:
            continue
        a = cols[1].find("a", href=True)
        if not a:
            continue
        nome = a["href"].strip()
        if not nome.lower().endswith(".csv"):
            continue
        last_mod = cols[2].get_text(strip=True)
        lista[nome] = last_mod
    return lista

def baixar_arquivo_csv(nome: str):
    """
    Faz download do CSV (bruto) de BASE_URL/nome para data/raw/nome.
    """
    os.makedirs(PASTA_RAW, exist_ok=True)
    url_csv = urljoin(BASE_URL, nome)
    caminho_local = os.path.join(PASTA_RAW, nome)
    resp = requests.get(url_csv, stream=True, timeout=30)
    resp.raise_for_status()
    with open(caminho_local, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

def popular_sqlite_rs(nome_arquivo: str):
    """
    Lê o CSV em data/raw/nome_arquivo, detecta a coluna de estado (Estado ou UF),
    filtra linhas onde esse campo == 'RS' e salva em tabela SQLite
    chamada '<nome_csv_sem_extensão>_rs'.
    """
    tabela_base = os.path.splitext(nome_arquivo)[0]            # ex: 'Comercio'
    tabela_rs = tabela_base.lower() + "_rs"                    # ex: 'comercio_rs'
    caminho_csv = os.path.join(PASTA_RAW, nome_arquivo)

    df = pd.read_csv(
        caminho_csv,
        sep=";",
        decimal=",",
        thousands=".",
        encoding="latin-1",
        low_memory=False
    )

    # 1) Detecta qual coluna indica o estado: procura 'estado' ou 'uf' (case-insensitive)
    cols_lower = [c.lower() for c in df.columns]
    if "estado" in cols_lower:
        state_col = df.columns[cols_lower.index("estado")]
    elif "uf" in cols_lower:
        state_col = df.columns[cols_lower.index("uf")]
    else:
        raise RuntimeError(f"CSV {nome_arquivo} não contém coluna 'Estado' nem 'UF'.")

        # Se encontrou coluna de estado, filtra só RS
    if state_col:
        df = df[df[state_col].astype(str).str.upper() == "RS"]

    # 3) Garante pasta de destino e carrega no SQLite
    os.makedirs(PASTA_PROC, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(tabela_rs, conn, if_exists="replace", index=False)

    # 4) Cria índice em 'Ano' se existir
    if "Ano" in df.columns:
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tabela_rs}_ano ON {tabela_rs}(Ano);")
    conn.close()

def treinar_modelo_forecast_rs():
    """
    Lê a tabela producao_rs do SQLite, agrupa por Ano somando Quantidade
    e treina um Prophet para prever produção de RS nos próximos 3 anos.
    Salva o modelo em models/forecast_producao_rs.pkl.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='producao_rs';")
    if not cursor.fetchone():
        conn.close()
        logging.warning("Tabela producao_rs não encontrada: pulando treino de forecast.")
        return

    df = pd.read_sql("SELECT Ano, SUM(Quantidade) AS QtdTotal FROM producao_rs GROUP BY Ano ORDER BY Ano ASC;", conn)
    conn.close()

    df_prophet = pd.DataFrame({
        "ds": pd.to_datetime(df["Ano"].astype(str) + "-01-01"),
        "y": df["QtdTotal"]
    })

    m = Prophet(yearly_seasonality=False, daily_seasonality=False, weekly_seasonality=False)
    m.fit(df_prophet)

    os.makedirs(MODELS_DIR, exist_ok=True)
    with open(FORECAST_MODEL_PATH, "wb") as f:
        pickle.dump(m, f)

    logging.info("Modelo de forecast de produção RS treinado e salvo.")

def atualizar_csvs_popular_db_e_treinar():
    """
    Orquestra:
    1. Cria tabela de usuários (login/senha) se necessário.
    2. Carrega state.json
    3. Baixa novos CSVs / atualizados
    4. Para cada CSV novo/atualizado, filtra e insere em SQLite (só RS)
    5. Se producao_rs mudou, re-treina modelo de forecast
    """
    criar_tabela_usuarios()

    estado_local = carregar_estado_local()
    lista_remota = obter_lista_remota()

    houve_atualizacao = False
    producao_atualizada = False

    for nome, last_mod_str in lista_remota.items():
        dt_remoto = parse_datetime(last_mod_str)
        if nome not in estado_local:
            baixar_arquivo_csv(nome)
            estado_local[nome] = last_mod_str
            logging.info(f"NOVO: {nome} ({last_mod_str})")

            popular_sqlite_rs(nome)
            logging.info(f"Tabela {os.path.splitext(nome)[0]}_rs criada no SQLite")

            houve_atualizacao = True
            if nome.lower().startswith("producao"):
                producao_atualizada = True

        else:
            dt_local = parse_datetime(estado_local[nome])
            if dt_remoto > dt_local:
                baixar_arquivo_csv(nome)
                estado_local[nome] = last_mod_str
                logging.info(f"ATUALIZADO: {nome} ({last_mod_str})")

                popular_sqlite_rs(nome)
                logging.info(f"Tabela {os.path.splitext(nome)[0]}_rs atualizada no SQLite")

                houve_atualizacao = True
                if nome.lower().startswith("producao"):
                    producao_atualizada = True
            else:
                logging.info(f"SEM MUDANÇA: {nome} (remoto={last_mod_str}, local={estado_local[nome]})")

    if houve_atualizacao:
        salvar_estado_local(estado_local)
        if producao_atualizada:
            try:
                treinar_modelo_forecast_rs()
            except Exception as e:
                logging.error(f"Erro ao treinar modelo de forecast: {e}")
    else:
        logging.info("Nenhum CSV modificado; nem o DB nem o modelo foram alterados.")

if __name__ == "__main__":
    atualizar_csvs_popular_db_e_treinar()