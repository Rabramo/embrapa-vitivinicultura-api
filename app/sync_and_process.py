#%% Importações

import os
import json
import logging
import requests
import sqlite3
import pandas as pd
import re

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from prophet import Prophet
import pickle


from passlib.context import CryptContext

#%% 1. CONFIGURAÇÕES GERAIS

BASE_URL = "http://vitibrasil.cnpuv.embrapa.br/download/"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
PASTA_RAW = os.path.join(DATA_DIR, "raw")
PASTA_PROC = os.path.join(DATA_DIR, "processed")
DB_PATH = os.path.join(PASTA_PROC, "embrapa.db")
ARQUIVO_STATE = os.path.join(os.path.dirname(__file__), "state.json")
MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
FORECAST_MODEL_PATH = os.path.join(MODELS_DIR, "forecast_producao_rs.pkl")
FORMATO_DATA = "%Y-%m-%d %H:%M"

#%% 2. CONFIGURAÇÃO DE LOG

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "update.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

#%% 3. HASH DE SENHA (PassLib)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

#%% 4. FUNÇÕES DE USUÁRIOS (SQLite)

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

#%% 5. FUNÇÕES AUXILIARES (estado local, parsing)

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

#%% 6. OBTÉM LISTA DE CSVs DISPONÍVEIS NO SITE

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

#%% 7. DOWNLOAD DE CSV

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

#%% 8. DETECTAR CABEÇALHO E LER O CSV

def detectar_indice_header(caminho_csv: str) -> int:
    """
    Abre o arquivo como texto e retorna o índice da linha
    onde aparece o cabeçalho verdadeiro. Para detectar:
      - Divide cada linha por ';'
      - Se a primeira célula (minusculizada) for 'uf', 'estado', 'id' ou 'produto',
        considera essa linha como header.
    Se não encontrar nada, retorna 0 (primeira linha).
    """
    with open(caminho_csv, "r", encoding="latin-1", errors="ignore") as f:
        for i, linha in enumerate(f):
            tokens = [t.strip().lower() for t in linha.split(";")]
            # Ajuste as chaves conforme o que aparece nos CSVs:
            if tokens and tokens[0] in ("uf", "estado", "id", "produto", "controle"):
                return i
    return 0

def ler_csv_embrapa(caminho_csv: str) -> pd.DataFrame:
    """
    Lê o arquivo CSV usando detecção de header + sep=';'.
    Se falhar, tenta sep=','.
    """
    header_row = detectar_indice_header(caminho_csv)
    try:
        df = pd.read_csv(
            caminho_csv,
            sep=";",
            encoding="latin-1",
            engine="python",
            header=header_row
        )
        # Se todo o header saiu vazio, cai no try-except e tenta vírgula
        if all(str(col).strip() == "" for col in df.columns):
            raise ValueError("Cabeçalho vazio com sep=';', header=" + str(header_row))
        return df
    except Exception as e1:
        logging.warning(f"Falha ler {os.path.basename(caminho_csv)} com sep=';': {e1}")

    # Tenta agora vírgula
    try:
        df = pd.read_csv(
            caminho_csv,
            sep=",",
            encoding="latin-1",
            engine="python",
            header=header_row
        )
        if all(str(col).strip() == "" for col in df.columns):
            raise ValueError("Cabeçalho vazio com sep=',', header=" + str(header_row))
        return df
    except Exception as e2:
        logging.error(f"Falha total ao ler {os.path.basename(caminho_csv)}: {e2}")
        raise

#%% 9. GRAVAR NO SQLITE (TRATAMENTO ESPECÍFICO PARA 'producao')


#%% 10. TREINAR MODELO DE FORECAST (TABELA 'producao')

def treinar_modelo_forecast_rs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='producao';"
    )
    if not cursor.fetchone():
        conn.close()
        logging.warning("Tabela 'producao' não encontrada: pulando treino de forecast.")
        return

    df = pd.read_sql("SELECT Ano, Quantidade FROM producao;", conn)
    conn.close()

    if df.empty:
        logging.warning("Tabela 'producao' vazia: pulando forecast.")
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

    logging.info("Modelo de forecast de produção treinado e salvo.")

#%% 11. FUNÇÃO PRINCIPAL

def atualizar_csvs_popular_db_e_treinar():
    # 0) Se não existir DB ou faltar 'producao', apaga state.json para forçar download
    if not os.path.isfile(DB_PATH):
        if os.path.exists(ARQUIVO_STATE):
            os.remove(ARQUIVO_STATE)
        logging.info("Banco ausente – removendo state.json para download inicial.")
    else:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='producao';"
        )
        if not cursor.fetchone():
            conn.close()
            if os.path.exists(ARQUIVO_STATE):
                os.remove(ARQUIVO_STATE)
            logging.info("Tabela 'producao' ausente – removendo state.json para forçar rebuild.")
        else:
            conn.close()

    # 1) Garante tabela users
    criar_tabela_usuarios()

    # 2) Carrega state.json e obtém lista remota
    estado_local = carregar_estado_local()
    lista_remota = obter_lista_remota()

    houve_atualizacao = False
    producao_atualizada = False

    for nome, last_mod_str in lista_remota.items():
        try:
            dt_remoto = parse_datetime(last_mod_str)
        except Exception:
            logging.warning(f"Formato data inválido para {nome}: '{last_mod_str}'. Pulando.")
            continue

        if nome not in estado_local:
            # Arquivo novo
            try:
                baixar_arquivo_csv(nome)
                estado_local[nome] = last_mod_str
                logging.info(f"NOVO CSV: {nome} ({last_mod_str})")
            except Exception as e:
                logging.error(f"Falha ao baixar {nome}: {e}")
                continue

            popular_sqlite(nome)
            if nome.lower().startswith("producao"):
                producao_atualizada = True
            houve_atualizacao = True

        else:
            # Já existe: compara timestamps
            try:
                dt_local = parse_datetime(estado_local[nome])
            except Exception:
                dt_local = None

            if dt_local is None or dt_remoto > dt_local:
                try:
                    baixar_arquivo_csv(nome)
                    estado_local[nome] = last_mod_str
                    logging.info(f"ATUALIZAÇÃO CSV: {nome} ({last_mod_str})")
                except Exception as e:
                    logging.error(f"Falha ao baixar {nome}: {e}")
                    continue

                popular_sqlite(nome)
                if nome.lower().startswith("producao"):
                    producao_atualizada = True
                houve_atualizacao = True
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
#%% 10. POPULAR SQLITE (função específica para 'producao')
def popular_sqlite(nome_arquivo: str):
    """
    Lê o CSV em data/raw/<nome_arquivo>, detecta o cabeçalho real via ler_csv_embrapa(),
    converte colunas-ano para inteiro e, se for 'Producao.csv', filtra RS + melt.
    Em seguida grava em tabela SQLite chamada '<nome_arquivo_sem_extensão>' (minúsculo).
    """
    caminho_csv = os.path.join(PASTA_RAW, nome_arquivo)
    if not os.path.isfile(caminho_csv):
        logging.error(f"[popular_sqlite] CSV não encontrado: {caminho_csv}")
        return

    # 1) Ler o CSV corretamente
    try:
        df = ler_csv_embrapa(caminho_csv)
        logging.info(f"[popular_sqlite] CSV lido: {nome_arquivo} ({len(df)} linhas)")
        logging.info(f"[popular_sqlite] Colunas encontradas em {nome_arquivo}: {list(df.columns)}")
    except Exception as e:
        logging.error(f"[popular_sqlite] Erro ao ler {nome_arquivo}: {e}")
        return

    # 2) Converter todas as colunas-ano (nome = quatro dígitos) para int
    col_anos = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c).strip())]
    for col in col_anos:
        # to_numeric coerces textos inválidos para NaN, depois fillna(0) e cast int
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        logging.info(f"[popular_sqlite] Coluna '{col}' convertida para INTEGER.")

    # 3) Tratamento específico para Producao.csv
    if nome_arquivo.lower().startswith("producao"):
        # 3.1) Detectar coluna de estado/UF
        state_col = None
        for col in df.columns:
            if str(col).strip().lower() in ("uf", "estado"):
                state_col = col
                break

        # 3.2) Se existir coluna de estado, filtrar somente as linhas de 'RS'
        if state_col:
            antes = len(df)
            df = df[df[state_col].astype(str).str.upper() == "RS"]
            logging.info(f"[popular_sqlite] Filtrado RS: {antes} → {len(df)} linhas")

        # 3.3) Derreter (melt) as colunas-ano para criar colunas 'Ano' e 'Quantidade'
        col_anos = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c).strip())]
        if not col_anos:
            logging.error("[popular_sqlite] Colunas de ano não encontradas em Producao.csv após leitura.")
            return

        df_melted = df.melt(
            id_vars=[state_col] if state_col else [],
            value_vars=col_anos,
            var_name="Ano",
            value_name="Quantidade"
        )
        # Garantir que 'Ano' seja tipo int
        df_melted["Ano"] = df_melted["Ano"].astype(int)
        df = df_melted

    # 4) Gravar no SQLite
    try:
        conn = sqlite3.connect(DB_PATH)
        base_name = os.path.splitext(nome_arquivo)[0].lower()
        df.to_sql(base_name, conn, if_exists="replace", index=False)
        conn.close()
        logging.info(f"[popular_sqlite] Tabela '{base_name}' criada/atualizada com {len(df)} registros.")
    except Exception as e:
        logging.error(f"[popular_sqlite] Erro ao gravar tabela '{base_name}': {e}")

#%% 12. ENTRYPOINT

if __name__ == "__main__":
    os.makedirs(PASTA_RAW, exist_ok=True)
    os.makedirs(PASTA_PROC, exist_ok=True)
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    atualizar_csvs_popular_db_e_treinar()
