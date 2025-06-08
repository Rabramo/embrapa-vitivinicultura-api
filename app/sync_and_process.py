#%% Importações
import os
import json
import logging
import requests
import sqlite3
import pandas as pd
import re
import unicodedata

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from prophet import Prophet
import pickle

from passlib.context import CryptContext

# CONFIGURAÇÕES GERAIS

BASE_URL = "http://vitibrasil.cnpuv.embrapa.br/download/"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
PASTA_RAW = os.path.join(DATA_DIR, "raw")
PASTA_PROC = os.path.join(DATA_DIR, "processed")
DB_PATH = os.path.join(PASTA_PROC, "embrapa.db")
ARQUIVO_STATE = os.path.join(os.path.dirname(__file__), "state.json")
MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
FORECAST_MODEL_PATH = os.path.join(MODELS_DIR, "forecast_producao_rs.pkl")
FORMATO_DATA = "%Y-%m-%d %H:%M"

# CONFIGURAÇÃO DE LOG

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "update.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# HASH DE SENHA (PassLib)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# USUÁRIOS (SQLite)

def criar_tabela_usuarios():
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

# AUXILIARES

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

def limpar_nome_coluna(nome):
    nome = str(nome)
    nome = unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("ASCII")
    nome = re.sub(r"\s+", "_", nome)
    nome = re.sub(r"[^\w]", "", nome)
    return nome.lower().strip("_")

# HTML: LISTA DE ARQUIVOS DISPONÍVEIS

def obter_lista_remota() -> dict:
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

# DOWNLOAD CSV

def baixar_arquivo_csv(nome: str):
    os.makedirs(PASTA_RAW, exist_ok=True)
    url_csv = urljoin(BASE_URL, nome)
    caminho_local = os.path.join(PASTA_RAW, nome)
    resp = requests.get(url_csv, stream=True, timeout=30)
    resp.raise_for_status()
    with open(caminho_local, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info(f"Download concluído: {nome}")

# LER CSV COM HEADER DETECTADO

def detectar_indice_header(caminho_csv: str) -> int:
    with open(caminho_csv, "r", encoding="latin-1", errors="ignore") as f:
        for i, linha in enumerate(f):
            tokens = [t.strip().lower() for t in linha.split(";")]
            if tokens and tokens[0] in ("uf", "estado", "id", "produto", "controle"):
                return i
    return 0

def ler_csv_embrapa(caminho_csv: str) -> pd.DataFrame:
    header_row = detectar_indice_header(caminho_csv)
    for sep in [';', ',']:
        try:
            df = pd.read_csv(
                caminho_csv,
                sep=sep,
                encoding="latin-1",
                engine="python",
                header=header_row
            )
            if all(str(col).strip() == "" for col in df.columns):
                raise ValueError("Cabeçalho vazio")
            df.columns = [limpar_nome_coluna(col) for col in df.columns]
            return df
        except Exception as e:
            logging.warning(f"Erro ao ler {caminho_csv} com sep '{sep}': {e}")
    raise ValueError(f"Erro ao ler CSV: {caminho_csv}")

# POPULAR SQLITE

def popular_sqlite(nome_arquivo: str):
    caminho_csv = os.path.join(PASTA_RAW, nome_arquivo)
    if not os.path.isfile(caminho_csv):
        logging.error(f"[popular_sqlite] CSV não encontrado: {caminho_csv}")
        return
    try:
        df = ler_csv_embrapa(caminho_csv)
        logging.info(f"[popular_sqlite] CSV lido: {nome_arquivo} ({len(df)} linhas)")
        logging.info(f"[popular_sqlite] Colunas: {list(df.columns)}")
    except Exception as e:
        logging.error(f"[popular_sqlite] Erro ao ler {nome_arquivo}: {e}")
        return

    col_anos = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c))]
    for col in col_anos:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if 'control' in df.columns and 'produto' in df.columns and col_anos:
        df = df.melt(id_vars=['control', 'produto'], value_vars=col_anos, var_name='Ano', value_name='Quantidade')
        df['Ano'] = pd.to_numeric(df['Ano'], errors='coerce').fillna(0).astype(int)

    base_name = os.path.splitext(nome_arquivo)[0].lower()
    try:
        conn = sqlite3.connect(DB_PATH)
        df.to_sql(base_name, conn, if_exists="replace", index=False)
        conn.close()
        logging.info(f"[popular_sqlite] Tabela '{base_name}' criada com {len(df)} registros.")
    except Exception as e:
        logging.error(f"[popular_sqlite] Erro ao gravar tabela '{base_name}': {e}")

# FORECAST

def treinar_modelo_forecast_rs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='producao';")
    if not cursor.fetchone():
        conn.close()
        logging.warning("Tabela 'producao' não encontrada")
        return

    df = pd.read_sql("SELECT Ano, Quantidade FROM producao;", conn)
    conn.close()

    if df.empty:
        logging.warning("Tabela 'producao' vazia")
        return

    df_agg = df.groupby("Ano", as_index=False)["Quantidade"].sum().sort_values("Ano")
    df_prophet = pd.DataFrame({
        "ds": pd.to_datetime(df_agg["Ano"].astype(str) + "-01-01"),
        "y": df_agg["Quantidade"]
    })

    m = Prophet(yearly_seasonality=False, daily_seasonality=False, weekly_seasonality=False)
    m.fit(df_prophet)

    os.makedirs(MODELS_DIR, exist_ok=True)
    with open(FORECAST_MODEL_PATH, "wb") as f:
        pickle.dump(m, f)
    logging.info("Modelo de forecast treinado e salvo")

# EXECUÇÃO PRINCIPAL

def atualizar_csvs_popular_db_e_treinar():
    if not os.path.isfile(DB_PATH):
        if os.path.exists(ARQUIVO_STATE):
            os.remove(ARQUIVO_STATE)
        logging.info("Banco ausente – removendo state.json para download inicial")
    else:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='producao';")
        if not cursor.fetchone():
            conn.close()
            if os.path.exists(ARQUIVO_STATE):
                os.remove(ARQUIVO_STATE)
            logging.info("Tabela 'producao' ausente – removendo state.json")
        else:
            conn.close()

    criar_tabela_usuarios()

    estado_local = carregar_estado_local()
    lista_remota = obter_lista_remota()
    houve_atualizacao = False
    producao_atualizada = False

    for nome, last_mod_str in lista_remota.items():
        try:
            dt_remoto = parse_datetime(last_mod_str)
        except Exception:
            logging.warning(f"Data inválida em {nome}: '{last_mod_str}'")
            continue

        if nome not in estado_local or parse_datetime(estado_local.get(nome, "1970-01-01 00:00")) < dt_remoto:
            try:
                baixar_arquivo_csv(nome)
                estado_local[nome] = last_mod_str
                popular_sqlite(nome)
                if nome.lower().startswith("producao"):
                    producao_atualizada = True
                houve_atualizacao = True
                logging.info(f"Atualização processada: {nome}")
            except Exception as e:
                logging.error(f"Erro ao processar {nome}: {e}")

    if houve_atualizacao:
        salvar_estado_local(estado_local)
        if producao_atualizada:
            try:
                treinar_modelo_forecast_rs()
            except Exception as e:
                logging.error(f"Erro ao treinar modelo: {e}")
    else:
        logging.info("Nenhum CSV atualizado")

if __name__ == "__main__":
    os.makedirs(PASTA_RAW, exist_ok=True)
    os.makedirs(PASTA_PROC, exist_ok=True)
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    atualizar_csvs_popular_db_e_treinar()

