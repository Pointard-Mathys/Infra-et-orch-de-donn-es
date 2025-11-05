import pandas as pd
import psycopg2
from hdfs import InsecureClient
import os
import time
import subprocess

LAST_ID_FILE = "/Hadoop/data/last_export.txt"

# Lecture du dernier ID exporté
try:
    with open(LAST_ID_FILE, "r") as f:
        last_id = int(f.read().strip())
except FileNotFoundError:
    last_id = 0

# Variables d'environnement Docker
host = os.environ.get("POSTGRES_HOST", "postgres")
port = int(os.environ.get("POSTGRES_PORT", 5432))
dbname = os.environ.get("POSTGRES_DB", "postgresdb")
user = os.environ.get("POSTGRES_USER", "postgresuser")
password = os.environ.get("POSTGRES_PASSWORD", "postgrespassword")

# Connexion PostgreSQL
conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)

# Sélection des nouvelles lignes
query = f"SELECT * FROM weapons WHERE id > {last_id}"
df = pd.read_sql(query, conn)

if not df.empty:
    df.to_csv("/Hadoop/data/weapons.csv", mode='a', header=not os.path.exists("/Hadoop/data/weapons.csv"), index=False)

    # Mise à jour du dernier ID exporté
    new_last_id = df['id'].max()
    with open(LAST_ID_FILE, "w") as f:
        f.write(str(new_last_id))

    print(f"{len(df)} nouvelles lignes exportées")
else:
    print("Pas de nouvelles lignes à exporter")

# Envoi vers HDFS
client = InsecureClient('http://hadoop-namenode:9870', user='root')
client.upload('/data/weapons.csv', '/Hadoop/data/weapons.csv', overwrite=True)
print("Fichier envoyé dans HDFS : /data/weapons.csv")

print("Lancement du job MapReduce...")
subprocess.run(["python", "run_hadoop_pipeline.py"], check=True)

print("Job MapReduce terminé. Insertion dans HBase...")
subprocess.run(["python", "insert_to_hbase.py"], check=True)

conn.close()
