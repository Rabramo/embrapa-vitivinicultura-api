# app/main.py
from fastapi import FastAPI

app = FastAPI(title="Embrapa Vitivinicultura API - Fase 1 FIAP")

@app.get("/")
async def root():
    return {"status": "OK", "mensagem": "API Embrapa Vitivinicultura no ar!"}
