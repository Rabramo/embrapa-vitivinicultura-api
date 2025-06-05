# app/auth.py

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from pydantic import BaseModel

# ===============================
# CONFIGURAÇÕES DE JWT
# ===============================
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "mudar_para_uma_chave_aleatoria_e_segura")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class UserIn(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

# O caminho para o banco SQLite (mesmo do pipeline)
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "embrapa.db")


def get_user(username: str):
    """
    Busca um usuário (username e hashed_password) na tabela 'users'.
    Retorna dicionário {"username": ..., "hashed_password": ...} ou None.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute("SELECT username, hashed_password FROM users WHERE username = ?;", (username,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"username": row["username"], "hashed_password": row["hashed_password"]}
    return None


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifica se a senha em texto plano bate com o hash Bcrypt armazenado.
    """
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str):
    """
    Retorna o dicionário do usuário se usuário e senha estiverem corretos;
    caso contrário, retorna False.
    """
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, user["hashed_password"]):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """
    Gera um JWT com o campo 'sub' = username e adiciona a expiração.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    """
    Dependência do FastAPI: decodifica o JWT, verifica validade e busca o usuário no banco.
    Se inválido, retorna HTTP 401.
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
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(current_user: dict = Depends(get_current_user)):
    """
    Dependência de rota: retorna o usuário autenticado (por enquanto,
    não há distinção entre ativo/inativo).
    """
    return current_user
