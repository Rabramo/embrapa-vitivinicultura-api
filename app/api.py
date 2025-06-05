# app/api.py

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordRequestForm
import sqlite3
import os
import pandas as pd
import pickle
from datetime import timedelta
from typing import List, Dict

from app.auth import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
    Token,
    ACCESS_TOKEN_EXPIRE_MINUTES
)

app = FastAPI(
    title="Embrapa Vitivinicultura no RS API com Auth",
    description="API para consultar dados de vitivinicultura no Rio Grande  e gerar forecast de produção.",
    version="1.0.0"
)


@app.on_event("startup")
async def on_startup():
    # Apenas importa a função; chamar faz a criação do BD e da tabela users
    from app.auth import init_db_and_users
    init_db_and_users()

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
    summary="Consulta Comercialização (RS) — intervalo de anos",
    response_model=List[Dict]
)
async def get_comercializacao(
    ano_inicio: int,
    ano_fim: int,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retorna todos os registros de comercialização (RS) entre ano_inicio e ano_fim (inclusive).
    """
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)
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
# GET /importacao?ano_inicio={X}&ano_fim={Y}
# =================================================
@app.get(
    "/importacao",
    summary="Consulta Importação (RS) — intervalo de anos",
    response_model=List[Dict]
)
async def get_importacao(
    ano_inicio: int,
    ano_fim: int,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retorna registros de importação (RS) entre ano_inicio e ano_fim (inclusive).
    """
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)
    query = """
        SELECT *
          FROM importacao
         WHERE Ano BETWEEN ? AND ?
         ORDER BY Ano ASC;
    """
    df = pd.read_sql(query, conn, params=(ano_inicio, ano_fim))
    conn.close()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum dado de importação encontrado para RS entre {ano_inicio} e {ano_fim}."
        )

    return df.to_dict(orient="records")


# =================================================
# GET /exportacao?ano_inicio={X}&ano_fim={Y}
# =================================================
@app.get(
    "/exportacao",
    summary="Consulta Exportação (RS) — intervalo de anos",
    response_model=List[Dict]
)
async def get_exportacao(
    ano_inicio: int,
    ano_fim: int,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retorna registros de exportação (RS) entre ano_inicio e ano_fim (inclusive).
    """
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")
    conn = sqlite3.connect(db_path)
    query = """
        SELECT *
          FROM exportacao
         WHERE Ano BETWEEN ? AND ?
         ORDER BY Ano ASC;
    """
    df = pd.read_sql(query, conn, params=(ano_inicio, ano_fim))
    conn.close()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum dado de exportação encontrado para RS entre {ano_inicio} e {ano_fim}."
        )

    return df.to_dict(orient="records")


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

