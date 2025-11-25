


# import subprocess
# import sys
# import os

# # --- CONFIGURATION ---
# HIVE_CONTAINER = "hadoop-hive"
# HDFS_CONTAINER = "hadoop-namenode"
# HDFS_INPUT = "/data/weapons.csv"
# HDFS_OUTPUT = "/output_weapons-00000"
# LOCAL_CSV = "/home/mathys/Infra-et-orch-de-donn-es/Hadoop/data/weapons.csv"
# HIVE_DB = "weapons"
# HIVE_TABLE = "results"

# # --- 1️⃣ Envoyer le fichier CSV vers HDFS ---
# print("📤 Upload du CSV vers HDFS...")
# try:
#     subprocess.run(
#         ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-mkdir", "-p", "/data"],
#         check=True
#     )
#     subprocess.run(
#         ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-put", "-f", LOCAL_CSV, HDFS_INPUT],
#         check=True
#     )
#     print(f"✅ Fichier {LOCAL_CSV} envoyé dans HDFS : {HDFS_INPUT}")
# except subprocess.CalledProcessError as e:
#     print("❌ Erreur lors de l'envoi vers HDFS :", e)
#     sys.exit(1)

# # --- 2️⃣ Lancer le job MapReduce (mapper + reducer) ---
# print("▶️ Lancement du job MapReduce...")
# try:
#     subprocess.run(
#         ["docker", "exec", HDFS_CONTAINER, "bash", "/run_hadoop_pipeline.sh"],
#         check=True
#     )
#     print(f"✅ Job MapReduce terminé, sortie HDFS : {HDFS_OUTPUT}")
# except subprocess.CalledProcessError as e:
#     print("❌ Erreur lors du job MapReduce :", e)
#     sys.exit(1)

# # --- 3️⃣ Lire le résultat depuis HDFS ---
# print("📄 Lecture des résultats depuis HDFS...")
# try:
#     result = subprocess.run(
#         ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-cat", HDFS_OUTPUT],
#         capture_output=True, text=True, check=True
#     )
# except subprocess.CalledProcessError as e:
#     print("❌ Erreur lors de la lecture HDFS :", e.stderr)
#     sys.exit(1)

# lines = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
# print(f"📄 {len(lines)} lignes récupérées depuis HDFS")

# # --- 4️⃣ Créer base et table Hive si elles n'existent pas ---
# print("📚 Création de la base et table Hive si nécessaire...")
# create_db_table_cmd = f"""
# CREATE DATABASE IF NOT EXISTS {HIVE_DB};
# CREATE TABLE IF NOT EXISTS {HIVE_DB}.{HIVE_TABLE} (
#     key STRING,
#     value STRING
# )
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY '\\t'
# STORED AS TEXTFILE;
# """
# subprocess.run(
#     ["docker", "exec", HIVE_CONTAINER, "hive", "-e", create_db_table_cmd],
#     check=True
# )
# print(f"✅ Base '{HIVE_DB}' et table '{HIVE_TABLE}' créées si inexistantes")

# # --- 5️⃣ Créer un fichier temporaire pour LOAD DATA ---
# temp_file = "temp_weapons.tsv"
# with open(temp_file, "w") as f:
#     for line in lines:
#         f.write(line + "\n")
# print(f"📄 Fichier temporaire créé : {temp_file}")

# # --- 6️⃣ Charger ce fichier dans HDFS pour Hive ---
# try:
#     subprocess.run(
#         ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-mkdir", "-p", f"/{HIVE_TABLE}"],
#         check=True
#     )
#     subprocess.run(
#         ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-put", "-f", temp_file, f"/{HIVE_TABLE}/part-00000"],
#         check=True
#     )
#     print(f"📦 Fichier chargé dans HDFS pour Hive : /{HIVE_TABLE}/part-00000")
# except subprocess.CalledProcessError as e:
#     print("❌ Erreur lors du put vers HDFS :", e)
#     sys.exit(1)

# # --- 7️⃣ Charger les données dans Hive ---
# load_cmd = f"LOAD DATA INPATH '/{HIVE_TABLE}/part-00000' INTO TABLE {HIVE_DB}.{HIVE_TABLE};"
# subprocess.run(
#     ["docker", "exec", HIVE_CONTAINER, "hive", "-e", load_cmd],
#     check=True
# )
# print(f"✅ {len(lines)} lignes insérées dans Hive : {HIVE_DB}.{HIVE_TABLE}")



import subprocess
import sys

HDFS_CONTAINER = "hadoop-namenode"
HIVE_CONTAINER = "hadoop-hive"
HDFS_INPUT = "/data/weapons.csv"
HIVE_DB = "weapons"
HIVE_TABLE = "results"

# --- Upload vers HDFS ---
try:
    subprocess.run(
        ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-mkdir", "-p", "/data"],
        check=True
    )
    subprocess.run(
        ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-put", "-f", HDFS_INPUT, "/data/weapons.csv"],
        check=True
    )
    print("✅ CSV chargé dans HDFS : /data/weapons.csv")
except subprocess.CalledProcessError as e:
    print("❌ Erreur lors de l'envoi vers HDFS :", e)
    sys.exit(1)

# --- Créer DB & Table Hive ---
create_cmd = f"""
CREATE DATABASE IF NOT EXISTS {HIVE_DB};
CREATE TABLE IF NOT EXISTS {HIVE_DB}.{HIVE_TABLE} (
    key STRING,
    value STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE;
"""

subprocess.run(["docker", "exec", HIVE_CONTAINER, "hive", "-e", create_cmd], check=True)
print(f"✅ Base {HIVE_DB} et table {HIVE_TABLE} créées")

# --- Charger les données dans Hive ---
load_cmd = f"LOAD DATA INPATH '/data/weapons.csv' INTO TABLE {HIVE_DB}.{HIVE_TABLE};"
subprocess.run(["docker", "exec", HIVE_CONTAINER, "hive", "-e", load_cmd], check=True)
print(f"✅ Données insérées dans Hive : {HIVE_DB}.{HIVE_TABLE}")
