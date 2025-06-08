import requests
import json
import unicodedata

def remover_acentos(texto):
    if isinstance(texto, str):
        nfkd = unicodedata.normalize('NFKD', texto)
        return ''.join(c for c in nfkd if not unicodedata.combining(c))
    return texto

def limpar_json(obj):
    if isinstance(obj, dict):
        return {remover_acentos(k): limpar_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [limpar_json(i) for i in obj]
    elif isinstance(obj, str):
        return remover_acentos(obj)
    return obj

def baixar_e_limpar_json(url: str):
    response = requests.get(url)
    response.encoding = 'utf-8'
    print("[DEBUG] Fazendo download...")
    return limpar_json(response.json())
