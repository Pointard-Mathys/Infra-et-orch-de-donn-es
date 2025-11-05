import happybase
import subprocess
import sys

# --- CONFIGURATION ---
HBASE_CONTAINER = "hbase"
THRIFT_PORT = 9090
TABLE_NAME = "weapons_results"
COLUMN_FAMILY = "cf"
HDFS_OUTPUT = "/output_weapons/part-00000"
HDFS_CONTAINER = "hadoop-namenode"

# --- LIRE LES RÉSULTATS MAPREDUCE DE HDFS ---
try:
    result = subprocess.run(
        ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-cat", HDFS_OUTPUT],
        capture_output=True, text=True, check=True
    )
except subprocess.CalledProcessError as e:
    print("❌ Erreur lors de la lecture HDFS :", e.stderr)
    sys.exit(1)

lines = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
print(f"📄 {len(lines)} lignes récupérées depuis HDFS")

# --- CONNEXION HBASE ---
connection = happybase.Connection(HBASE_CONTAINER, port=THRIFT_PORT)
connection.open()
print("🔗 Connexion HBase ouverte :", connection.is_open)

# --- CRÉER TABLE SI INEXISTANTE ---
if TABLE_NAME.encode() not in connection.tables():
    connection.create_table(TABLE_NAME, {COLUMN_FAMILY: dict()})
    print(f"✅ Table '{TABLE_NAME}' créée avec la colonne '{COLUMN_FAMILY}'")
else:
    print(f"ℹ️ Table '{TABLE_NAME}' existe déjà")

table = connection.table(TABLE_NAME)

# --- INSÉRER LES DONNÉES ---
for i, line in enumerate(lines):
    try:
        key, value = line.split('\t', 1)
        table.put(key.encode(), {f"{COLUMN_FAMILY}:value".encode(): value.encode()})
    except Exception as e:
        print(f"⚠️ Ligne ignorée [{line}]: {e}")

print(f"✅ {len(lines)} lignes insérées dans HBase : {TABLE_NAME}")
connection.close()
