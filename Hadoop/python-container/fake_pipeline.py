import csv
from collections import defaultdict
from flask import Flask, Response
import threading

INPUT_CSV = "/app/data/weapons.csv"

# ---------------------------------------------------------
# Lecture CSV + MapReduce
# ---------------------------------------------------------
def read_csv():
    rows = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for line in reader:
            if len(line) < 7:
                continue
            rows.append(line)
    return rows

def map_reduce(rows):
    counts = defaultdict(int)
    for row in rows:
        element = row[6]
        if element.strip():
            counts[element] += 1
    return counts

# ---------------------------------------------------------
# Générer les metrics Prometheus
# ---------------------------------------------------------
def generate_metrics(counts):
    metrics = ""
    total_weapons = sum(counts.values())
    metrics += f"pipeline_total_weapons {total_weapons}\n"
    for elem, val in counts.items():
        metrics += f'pipeline_weapon_count{{element="{elem}"}} {val}\n'
    return metrics

# ---------------------------------------------------------
# Serveur Flask
# ---------------------------------------------------------
app = Flask(__name__)
METRICS = ""

@app.route("/metrics")
def metrics_endpoint():
    return Response(METRICS, mimetype="text/plain")

def run_flask():
    app.run(host="0.0.0.0", port=5000)

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    rows = read_csv()
    counts = map_reduce(rows)
    METRICS = generate_metrics(counts)

    # Flask dans un thread pour éviter de bloquer
    threading.Thread(target=run_flask).start()







# import csv
# import sqlite3
# from collections import defaultdict

# INPUT_CSV = "/home/mathys/Infra-et-orch-de-donn-es/Hadoop/data/weapons.csv"
# DB_NAME = "fake_hive.db"


# # ---------------------------------------------------------
# # 1) LECTURE DU CSV
# # ---------------------------------------------------------
# def read_csv():
#     print("📥 Lecture du CSV...")
#     rows = []

#     with open(INPUT_CSV, "r", encoding="utf-8") as f:
#         reader = csv.reader(f)
#         header = next(reader)  # sauter les en-têtes

#         for line in reader:
#             if len(line) < 7:
#                 continue
#             rows.append(line)

#     print(f"✔ {len(rows)} lignes chargées")
#     return rows


# # ---------------------------------------------------------
# # 2) MAP + REDUCE interne Python (sans Hadoop)
# # ---------------------------------------------------------
# def map_reduce(rows):
#     print("⚙️ Exécution du MapReduce...")

#     counts = defaultdict(int)

#     for row in rows:
#         element = row[6]  # colonne "element"
#         if element.strip():
#             counts[element] += 1

#     print("✔ MapReduce terminé")
#     return counts


# # ---------------------------------------------------------
# # 3) SIMULATION HIVE avec SQLite
# # ---------------------------------------------------------
# def hive_insert(counts):
#     print("🗃  Connexion à la base Hive simulée...")

#     conn = sqlite3.connect(DB_NAME)
#     cur = conn.cursor()

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS weapon_stats (
#             element TEXT,
#             total INT
#         )
#     """)

#     cur.execute("DELETE FROM weapon_stats")

#     for elem, total in counts.items():
#         cur.execute("INSERT INTO weapon_stats (element, total) VALUES (?, ?)", (elem, total))

#     conn.commit()
#     print("✔ Données insérées dans la base Hive fake")
#     return conn, cur


# # ---------------------------------------------------------
# # 4) AFFICHAGE DES DONNÉES HIVE FAKE
# # ---------------------------------------------------------
# def show_results(cur):
#     print("\n📊 Contenu final de la table (FAKE HIVE) :\n")

#     for element, total in cur.execute("SELECT element, total FROM weapon_stats ORDER BY total DESC"):
#         print(f"{element:<10} → {total} armes")


# # ---------------------------------------------------------
# # 5) ÉCRITURE DES METRICS PROMETHEUS
# # ---------------------------------------------------------
# def write_metrics(counts):
#     total_weapons = sum(counts.values())
#     avg_attack = total_weapons / len(counts) if counts else 0

#     with open("metrics.prom", "w") as f:
#         f.write(f"pipeline_total_weapons {total_weapons}\n")
#         f.write(f"pipeline_average_attack {avg_attack}\n")
#         for element, total in counts.items():
#             f.write(f'pipeline_weapon_count{{element="{element}"}} {total}\n')

#     print("✔ Metrics Prometheus écrites dans metrics.prom")


# # ---------------------------------------------------------
# # PIPELINE COMPLET
# # ---------------------------------------------------------
# def main():
#     rows = read_csv()
#     counts = map_reduce(rows)
#     conn, cur = hive_insert(counts)
#     show_results(cur)
#     write_metrics(counts)
#     conn.close()
#     print("\n🎉 Pipeline complet exécuté et metrics générées.\n")


# if __name__ == "__main__":
#     main()
