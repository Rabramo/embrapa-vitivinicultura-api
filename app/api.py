# app/api.py

from fastapi import FastAPI, Query, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from app.sanitize_api_response import limpar_json
import sqlite3
import os
import pandas as pd
import pickle
from datetime import timedelta
from typing import List, Dict, Optional
from app.sync_and_process import popular_sqlite

from app.auth import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
    Token,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    init_db_and_users
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializa usuários e banco de autenticação
    init_db_and_users()

    # Lista de URLs dos arquivos a baixar
    urls = [
        "https://vitibrasil.cnpuv.embrapa.br/download/Producao.csv",
        "https://vitibrasil.cnpuv.embrapa.br/download/Uva.csv",
        "https://vitibrasil.cnpuv.embrapa.br/download/Vinho.csv",
        "https://vitibrasil.cnpuv.embrapa.br/download/Suco.csv",
        "https://vitibrasil.cnpuv.embrapa.br/download/ComercioExterior.csv"
    ]

    # Limpa cada JSON
    for url in urls:
        limpar_json(url)

    # Lê CSVs e popular o SQLite embrapa.db
    popular_sqlite()

    yield

    # Cleanup: apaga arquivos temporários em data/raw
    raw_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    if os.path.exists(raw_path):
        for f in os.listdir(raw_path):
            if f.endswith(".json") or f.endswith(".csv"):
                os.remove(os.path.join(raw_path, f))
        print("[lifespan] Arquivos temporários removidos de data/raw.")

app = FastAPI(
    title="Embrapa Vitivinicultura no RS API com Auth",
    description="API para consultar dados de vitivinicultura no Rio Grande e gerar forecast de produção.",
    version="1.0.0",
    lifespan=lifespan
)


# Lista segura de tabelas válidas
TABELAS_VALIDAS = {
    "expvinho", "expsuco", "expuva", "expespumantes","impvinhos",
    "impfrescas", "impespumantes", "imppassas", "impvinhos", "impsucos"
}


# =========================================
# POST /token  → Autenticação e JWT
# =========================================
@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Recebe username e password via form‐data e retorna o JWT (access_token).
    """
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário ou senha incorretos",
            headers={"WWW-Authenticate": "Bearer"},
        )
    expire_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user["username"]}, expires_delta=expire_delta)
    return {"access_token": access_token, "token_type": "bearer"}


# =================================================
# GET /producao?ano_inicio={X}&ano_fim={Y}
# =================================================
@app.get(
    "/producao",
    summary="Consulta Produção de Uvas — intervalo de anos",
    response_model=List[Dict]
)
async def get_producao(
    ano_inicio: int,
    ano_fim: int,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retorna todos os registros de produção de uvas no RS entre ano_inicio e ano_fim (inclusive).
    """
    # Caminho para o SQLite (pipeline já criou a tabela producao_rs)
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)
    query = """
        SELECT *
          FROM producao
         WHERE Ano BETWEEN ? AND ?
         ORDER BY Ano ASC;
    """
    df = pd.read_sql(query, conn, params=(ano_inicio, ano_fim))
    conn.close()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum dado de produção encontrado para RS entre {ano_inicio} e {ano_fim}."
        )

    return df.to_dict(orient="records")


# =================================================
# GET /comercializacao?ano_inicio={X}&ano_fim={Y}
# =================================================
@app.get(
    "/comercializacao",
    summary="Consulta Comercialização (RS) — intervalo de anos e produto",
    response_model=List[Dict]
)
async def get_comercializacao(
    ano_inicio: int,
    ano_fim: int,
    produto: Optional[str] = None,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retorna todos os registros de comercialização (RS) entre ano_inicio e ano_fim (inclusive),
    com filtro opcional por produto.
    """
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)

    if produto:
        query = """
            SELECT *
              FROM comercio
             WHERE Ano BETWEEN ? AND ?
               AND produto = ?
             ORDER BY Ano ASC;
        """
        df = pd.read_sql(query, conn, params=(ano_inicio, ano_fim, produto))
    else:
        query = """
            SELECT *
              FROM comercio
             WHERE Ano BETWEEN ? AND ?
             ORDER BY Ano ASC;
        """
        df = pd.read_sql(query, conn, params=(ano_inicio, ano_fim))

    conn.close()


    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum dado de comercialização encontrado para RS entre {ano_inicio} e {ano_fim}."
        )

    return df.to_dict(orient="records")


# ============================================================
# GET /processamento?ano_inicio={X}&ano_fim={Y}&tipo={Z}
# ============================================================
@app.get(
    "/processamento",
    summary="Consulta Processamento (RS) — intervalo de anos e tipo",
    response_model=List[Dict]
)
async def get_processamento(
    ano_inicio: int,
    ano_fim: int,
    tipo: str,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retorna registros de processamento (RS) para o tipo informado entre ano_inicio e ano_fim.
    'tipo' pode ser: "Viniferas", "Mesa", "Americanas", "Semclass".
    """
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)
    query = """
        SELECT *
          FROM processamento
         WHERE Ano BETWEEN ? AND ?
           AND UPPER(tipo) = UPPER(?)
         ORDER BY Ano ASC;
    """
    df = pd.read_sql(query, conn, params=(ano_inicio, ano_fim, tipo))
    conn.close()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Nenhum dado de processamento do tipo '{tipo}' "
                f"encontrado para RS entre {ano_inicio} e {ano_fim}."
            )
        )

    return df.to_dict(orient="records")

# =================================================
# GET /comex?produto={X}&ano_inicio={Y}&ano_fim={Z}
# =================================================

# @app.get("/comex", summary="Consulta dados de importação/exportação por produto e ano")
async def get_comex(
    produto: str = Query(..., description="Nome da tabela, ex: expvinho, imppassas"),
    ano_inicio: int = Query(...),
    ano_fim: int = Query(...),
):
    if produto not in TABELAS_VALIDAS:
        raise HTTPException(status_code=400, detail="Produto inválido")

    # Caminho do banco
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)

    try:
        df = pd.read_sql_query(f"SELECT * FROM {produto}", conn)
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    conn.close()

    # Transforma em formato longo
    df_long = df.melt(id_vars=df.columns[0:2], var_name="ano", value_name="valor")
    df_long["ano"] = pd.to_numeric(df_long["ano"], errors="coerce")

    # Filtra por intervalo de anos
    df_filtrado = df_long[df_long["ano"].between(ano_inicio, ano_fim)]

    if df_filtrado.empty:
        raise HTTPException(status_code=404, detail="Nenhum dado encontrado")

    return df_filtrado.to_dict(orient="records")

# =================================================
# GET /forecast/producao?periodos={N}
# =================================================
@app.get(
    "/forecast/producao",
    summary="Forecast de Produção (RS)",
    response_model=Dict
)
async def get_forecast_producao(
    periodos: int = 3,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retorna:
      - 'historico': lista de { ano: int, producao: float } para todos os anos no DB
      - 'forecast': lista de { ano: int, previsao: float, intervalo_80_inf: float, intervalo_80_sup: float }
        para os próximos 'periodos' anos (a partir do último ano histórico disponível).
    """
    # 1) Obter histórico de produção do RS
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        "SELECT Ano, SUM(Quantidade) AS QtdTotal FROM producao GROUP BY Ano ORDER BY Ano ASC;",
        conn
    )
    conn.close()

    if df.empty:
        raise HTTPException(status_code=404, detail="Tabela producao está vazia ou não existe.")

    # Montar lista de histórico
    historico = [{"ano": int(row["Ano"]), "producao": float(row["QtdTotal"])} for _, row in df.iterrows()]

    # 2) Carregar modelo Prophet salvo
    model_path = os.path.join(os.path.dirname(__file__), "..", "models", "forecast_producao_rs.pkl")
    if not os.path.isfile(model_path):
        raise HTTPException(status_code=404, detail="Modelo de forecast não encontrado.")

    with open(model_path, "rb") as f:
        m = pickle.load(f)

    # 3) Preparar DataFrame para Prophet
    #    (embora não usemos diretamente, apenas para alinhar datas)
    df_prophet = pd.DataFrame({
        "ds": pd.to_datetime(df["Ano"].astype(str) + "-01-01"),
        "y": df["QtdTotal"]
    })

    # 4) Criar DataFrame futuro e obter previsões
    futuro = m.make_future_dataframe(periods=periodos, freq="Y")
    previsao = m.predict(futuro)

    # 5) Extrair somente os anos futuros além do último ano histórico
    ult_ano = int(df["Ano"].max())
    forecast_list = []
    for _, row in previsao.iterrows():
        ano_pred = row["ds"].year
        if ano_pred > ult_ano:
            forecast_list.append({
                "ano": int(ano_pred),
                "previsao": float(row["yhat"]),
                "intervalo_80_inf": float(row["yhat_lower"]),
                "intervalo_80_sup": float(row["yhat_upper"])
            })

    # 6) Garantir que retornamos apenas 'periodos' itens
    forecast_list = forecast_list[:periodos]

    return {"historico": historico, "forecast": forecast_list}

# =======================================================================
# GET /openapi-limpo → Retorna OpenAPI sem acentos e caracteres especiais
# =======================================================================

@app.get("/openapi-limpo")
def openapi_limpo():
    url = "https://embrapa-vit-api.onrender.com/openapi.json"
    json_limpo = limpar_json(url)
    return JSONResponse(content=json_limpo)