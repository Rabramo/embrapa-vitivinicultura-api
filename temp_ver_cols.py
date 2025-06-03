import pandas as pd
import os

caminho = os.path.join("data", "raw", "Comercio.csv")
df = pd.read_csv(
    caminho,
    sep=";",
    decimal=",",
    thousands=".",
    encoding="latin-1",
    nrows=0  # só carrega o cabeçalho
)
print("Colunas encontradas em Comercio.csv:")
print(df.columns.tolist())
