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


    # 1. Download CSV da URL
    resposta = requests.get(url)
    if resposta.status_code != 200:
        raise Exception(f"Erro ao baixar {url}: {resposta.status_code}")
    
    caminho_csv = os.path.join(raw_dir, f"{nome_arquivo}.csv")

    # 2. Grava o conteúdo em arquivo
    with open(caminho_csv, "wb") as f:
        f.write(resposta.content)
    
    print(f"[OK] CSV baixado e salvo em: {caminho_csv}")