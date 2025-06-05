#%%
import pandas as pd

df = pd.read_csv(
    "data/raw/Comercio.csv",
    sep=";",
    decimal=",",
    thousands=".",
    encoding="latin-1"
)
# %%
print("Colunas encontradas em Comercio.csv:")
print(df.columns.tolist())
# %%
df.head()
# %%
df.info()
# %%
