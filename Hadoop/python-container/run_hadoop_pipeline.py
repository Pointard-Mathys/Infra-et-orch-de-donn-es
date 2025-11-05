import subprocess

MAPPER = "processing/mapper.py"
REDUCER = "processing/reducer.py"
HDFS_CSV = "/data/weapons.csv"
OUTPUT_DIR = "/output_weapons"

print("📄 Copie Mapper & Reducer dans le conteneur Hadoop...")
subprocess.run(["docker", "cp", MAPPER, "hadoop-namenode:/mapper.py"])
subprocess.run(["docker", "cp", REDUCER, "hadoop-namenode:/reducer.py"])
print("✅ Scripts MapReduce transférés")

print("⚙️  Exécution du job MapReduce...")
subprocess.run([
    "docker", "exec", "hadoop-namenode", "bash", "-c",
    f"hdfs dfs -rm -r {OUTPUT_DIR} || true && "
    f"hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar "
    f"-input {HDFS_CSV} -output {OUTPUT_DIR} -mapper /mapper.py -reducer /reducer.py"
])
print("✅ Job MapReduce terminé")

print("📦 Résultat du traitement :\n")
subprocess.run([
    "docker", "exec", "hadoop-namenode", "bash", "-c",
    f"hdfs dfs -cat {OUTPUT_DIR}/part-00000"
])
