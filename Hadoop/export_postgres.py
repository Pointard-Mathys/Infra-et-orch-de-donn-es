import pandas as pd
import psycopg2
from hdfs import InsecureClient
import os

# Connexion PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=5432,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

print("📦 Connexion PostgreSQL réussie.")
df = pd.read_sql("SELECT * FROM weapons", conn)
local_path = "data/weapons.csv"
df.to_csv(local_path, index=False)
print(f"Fichier CSV exporté localement : {local_path}")

# Envoi vers HDFS
client = InsecureClient('http://hadoop-namenode:9870', user='root')
client.upload('/data/weapons.csv', local_path, overwrite=True)
print("Fichier envoyé dans HDFS : /data/weapons.csv")

conn.close()
