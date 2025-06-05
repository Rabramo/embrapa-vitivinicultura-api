# app/auth.py

import os
import sqlite3
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from pydantic import BaseModel

# ===============================
# CAMINHO PARA O SQLite EMBRAPA.DB
# ===============================

BASE_DIR = os.path.dirname(__file__)                  # aponta para .../app
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
PASTA_PROC = os.path.join(DATA_DIR, "processed")
DB_PATH = os.path.join(PASTA_PROC, "embrapa.db")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# ===============================
# CONFIGURAÇÕES DE JWT
# ===============================
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "mudar_para_uma_chave_aleatoria_e_segura")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# -----------------------------------------------------
# Modelo Pydantic para resposta de Token (em api.py)
# -----------------------------------------------------
from pydantic import BaseModel

class Token(BaseModel):
    access_token: str
    token_type: str

def init_db_and_users():
    """
    Cria o diretório data/processed (se faltar), 
    abre/gera embrapa.db, cria tabela 'users' (se não existir)
    e garante que haja o usuário admin/admin123.
    """
    # 1) Garante que a pasta data/processed exista
    os.makedirs(PASTA_PROC, exist_ok=True)

    # 2) Conecta (ou cria) o arquivo embrapa.db
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 3) Cria a tabela users se não existir
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            hashed_password TEXT NOT NULL
        );
        """
    )

    # 4) Insere o usuário admin com senha "admin123" (hash) se ainda não existir
    #    Geramos aqui o hash para "admin123"
    hashed = pwd_context.hash("admin123")
    cursor.execute(
        """
        INSERT OR IGNORE INTO users(username, hashed_password)
        VALUES (?, ?);
        """,
        ("admin", hashed)
    )

    conn.commit()
    conn.close()

def get_user(username: str) -> Optional[dict]:
    """
    Retorna um dicionário {"username": ..., "hashed_password": ...} se o usuário existir,
    ou None caso não exista.
    Antes de fazer o SELECT, garante que a tabela users (e o admin) existam.
    """
    # 1) Garante que o BD e a tabela users estejam criados
    init_db_and_users()

    # 2) Agora conecta para buscar dados do username
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        "SELECT username, hashed_password FROM users WHERE username = ?;",
        (username,)
    )
    row = cursor.fetchone()
    conn.close()

    if row:
        return {"username": row[0], "hashed_password": row[1]}
    return None



def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifica se a senha em texto plano bate com o hash Bcrypt armazenado.
    """
    return pwd_context.verify(plain_password, hashed_password)

def authenticate_user(username: str, password: str) -> Optional[dict]:
    """
    Verifica se o usuário existe e se a senha bate com o hash.
    Retorna o dict de usuário ou None se inválido.
    """
    user = get_user(username)
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """
    Gera um JWT com os dados (payload) e expiração.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(OAuth2PasswordBearer(tokenUrl="token"))):
    """
    Valida o JWT e retorna o usuário (dictionary) se estiver válido.
    Senão, lança 401.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Não foi possível validar credenciais",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = get_user(username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(current_user: dict = Depends(get_current_user)):
    """
    Dependência de rota: retorna o usuário autenticado (por enquanto,
    não há distinção entre ativo/inativo).
    """
    return current_user
