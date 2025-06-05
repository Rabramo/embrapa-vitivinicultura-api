# 1) Base: Python 3.11 slim
FROM python:3.11

# 2) Variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV JWT_SECRET_KEY="mudar_para_prod_chave_secreta"

# 3) Instalar dependências de sistema para compilar PyStan/Prophet e pacote científico
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        make \
        libgomp1 \
        python3-dev \
        libatlas-base-dev \
        git && \
    rm -rf /var/lib/apt/lists/*

# 4) Definir diretório de trabalho
WORKDIR /app

# 5) Copiar e instalar dependências Python, garantindo NumPy 1º
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# 6) Copiar o restante do código para dentro da imagem
COPY . /app

# 7) Criar pastas de dados/formato/logs/modelos
RUN mkdir -p /app/data/raw \
    && mkdir -p /app/data/processed \
    && mkdir -p /app/logs \
    && mkdir -p /app/models

# 8) Expor porta 8000 para Uvicorn
EXPOSE 8000

# 9) Comando que roda pipeline e, depois, a API
CMD ["/bin/bash", "-lc", "\
    python /app/app/sync_and_process.py && \
    uvicorn app.api:app --host 0.0.0.0 --port 8000 \
"]
