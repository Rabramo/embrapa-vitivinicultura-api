# Embrapa Vitivinicultura API

[Servidor roda cron diário ou container frio] 
   └─> sync_and_process.py (baixa CSV ↠ popula SQLite  ↠ treina modelo Prophet) 
          ↳ logs/update.log
   └─> uvicorn app.api:app (FastAPI) serve endpoints protegidos por JWT
          ↳ logs/access.log 

