# import subprocess

# MAPPER = "processing/mapper.py"
# REDUCER = "processing/reducer.py"
# HDFS_CSV = "/data/weapons.csv"
# OUTPUT_DIR = "/output_weapons"

# print("📄 Copie Mapper & Reducer dans le conteneur Hadoop...")
# subprocess.run(["docker", "cp", MAPPER, "hadoop-namenode:/mapper.py"])
# subprocess.run(["docker", "cp", REDUCER, "hadoop-namenode:/reducer.py"])
# print("✅ Scripts MapReduce transférés")

# print("⚙️  Exécution du job MapReduce...")
# subprocess.run([
#     "docker", "exec", "hadoop-namenode", "bash", "-c",
#     f"hdfs dfs -rm -r {OUTPUT_DIR} || true && "
#     f"hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar "
#     f"-input {HDFS_CSV} -output {OUTPUT_DIR} -mapper /mapper.py -reducer /reducer.py"
# ])
# print("✅ Job MapReduce terminé")

# print("📦 Résultat du traitement :\n")
# subprocess.run([
#     "docker", "exec", "hadoop-namenode", "bash", "-c",
#     f"hdfs dfs -cat {OUTPUT_DIR}/part-00000"
# ])



# import subprocess

# MAPPER = "processing/mapper.py"
# REDUCER = "processing/reducer.py"
# HDFS_CSV = "/data/weapons.csv"
# OUTPUT_DIR = "/output_weapons"
# HADOOP_STREAMING_JAR = "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"



# # --- Copier Mapper & Reducer ---
# print("📄 Copie Mapper & Reducer dans le conteneur Hadoop...")
# subprocess.run(["docker", "cp", MAPPER, "hadoop-namenode:/mapper.py"], check=True)
# subprocess.run(["docker", "cp", REDUCER, "hadoop-namenode:/reducer.py"], check=True)
# print("✅ Scripts MapReduce transférés")

# # --- Exécuter le job MapReduce ---
# print("⚙️  Exécution du job MapReduce...")
# subprocess.run([
#     "docker", "exec", "hadoop-namenode", "bash", "-c",
#     f"hdfs dfs -rm -r {OUTPUT_DIR} || true && "
#     f"hadoop jar {HADOOP_STREAMING_JAR} "
#     f"-input {HDFS_CSV} -output {OUTPUT_DIR} -mapper /mapper.py -reducer /reducer.py"
# ], check=True)
# print("✅ Job MapReduce terminé")

# # --- Vérifier le résultat ---
# print("📦 Résultat du traitement :")
# subprocess.run([
#     "docker", "exec", "hadoop-namenode", "hdfs", "dfs", "-cat", f"{OUTPUT_DIR}/part-00000"
# ])



import subprocess
import time
import os

# Détermination des chemins réels
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MAPPER_SRC = os.path.join(BASE_DIR, "processing", "mapper.py")
REDUCER_SRC = os.path.join(BASE_DIR, "processing", "reducer.py")
CSV_SRC = os.path.join(BASE_DIR, "..", "data", "weapons.csv")

print("📄 Copie Mapper & Reducer dans le conteneur Hadoop...")
subprocess.run([
    "docker", "cp", MAPPER_SRC, "hadoop-namenode:/mapper.py"
], check=True)
subprocess.run([
    "docker", "cp", REDUCER_SRC, "hadoop-namenode:/reducer.py"
], check=True)
print("✅ Scripts MapReduce transférés")

print("📄 Préparation du dossier /data dans le conteneur...")
subprocess.run([
    "docker", "exec", "hadoop-namenode", "mkdir", "-p", "/data"
], check=True)
print("📁 Dossier /data créé")

print("📄 Copie du fichier CSV dans le conteneur Hadoop...")
subprocess.run([
    "docker", "cp", CSV_SRC, "hadoop-namenode:/data/weapons.csv"
], check=True)
print("✅ CSV transféré")

print("📤 Envoi du CSV dans HDFS...")
subprocess.run([
    "docker", "exec", "hadoop-namenode", "bash", "-c",
    "hdfs dfs -mkdir -p /data && hdfs dfs -put -f /data/weapons.csv /data/weapons.csv"
], check=True)
print("✅ CSV chargé dans HDFS")

print("⚙️  Exécution du job MapReduce...")
subprocess.run([
    "docker", "exec", "hadoop-namenode", "bash", "-c",
    "hdfs dfs -rm -r /output_weapons || true && "
    "hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar "
    "-input /data/weapons.csv -output /output_weapons "
    "-mapper /mapper.py -reducer /reducer.py"
], check=True)

print("🎉 Job MapReduce terminé !")

