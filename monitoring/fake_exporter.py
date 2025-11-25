import csv
import sqlite3
from collections import defaultdict
from flask import Flask, Response
import threading

INPUT_CSV = "/home/mathys/Infra-et-orch-de-donn-es/Hadoop/data/weapons.csv"
DB_NAME = "fake_hive.db"

# ---------------------------------------------------------
# 1) Lecture du CSV
# ---------------------------------------------------------
def read_csv():
    rows = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # sauter header
        for line in reader:
            if len(line) < 7:
                continue
            rows.append(line)
    return rows

# ---------------------------------------------------------
# 2) MapReduce interne
# ---------------------------------------------------------
def map_reduce(rows):
    counts = defaultdict(int)
    total_attack = 0
    for row in rows:
        element = row[6].strip()
        if element:
            counts[element] += 1
        try:
            attack = float(row[2])  # colonne "attaque" exemple
            total_attack += attack
        except:
            continue
    avg_attack = total_attack / len(rows) if rows else 0
    return counts, avg_attack

# ---------------------------------------------------------
# 3) Simulation Hive avec SQLite
# ---------------------------------------------------------
def hive_insert(counts):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weapon_stats (
            element TEXT,
            total INT
        )
    """)
    cur.execute("DELETE FROM weapon_stats")
    for elem, total in counts.items():
        cur.execute("INSERT INTO weapon_stats (element, total) VALUES (?, ?)", (elem, total))
    conn.commit()
    return conn, cur

# ---------------------------------------------------------
# 4) Génération des metrics pour Prometheus
# ---------------------------------------------------------
def generate_metrics(counts, avg_attack):
    total_weapons = sum(counts.values())
    metrics = f"pipeline_total_weapons {total_weapons}\n"
    metrics += f"pipeline_average_attack {avg_attack}\n"
    for element, count in counts.items():
        metrics += f'pipeline_weapon_count{{element="{element}"}} {count}\n'
    return metrics

# ---------------------------------------------------------
# 5) Flask app pour exposer les metrics
# ---------------------------------------------------------
app = Flask(__name__)
METRICS = ""

@app.route("/metrics")
def metrics_endpoint():
    return Response(METRICS, mimetype="text/plain")

def run_flask():
    app.run(host="0.0.0.0", port=5000)

# ---------------------------------------------------------
# 6) Pipeline complet
# ---------------------------------------------------------
def main():
    global METRICS
    rows = read_csv()
    counts, avg_attack = map_reduce(rows)
    conn, cur = hive_insert(counts)
    METRICS = generate_metrics(counts, avg_attack)
    print("🚀 Pipeline prêt, metrics exposées sur http://0.0.0.0:5000/metrics")
    conn.close()
    run_flask()

if __name__ == "__main__":
    main()









# from flask import Flask, Response
# import csv
# import sqlite3
# from collections import defaultdict

# INPUT_CSV = "/data/weapons.csv"  # on mappe le CSV dans Docker
# DB_NAME = "fake_hive.db"

# app = Flask(__name__)

# # -----------------------------
# # Pipeline fake Hive
# # -----------------------------
# def read_csv():
#     rows = []
#     with open(INPUT_CSV, "r", encoding="utf-8") as f:
#         reader = csv.reader(f)
#         header = next(reader)
#         for line in reader:
#             if len(line) < 7:
#                 continue
#             rows.append(line)
#     return rows

# def map_reduce(rows):
#     counts = defaultdict(int)
#     total_attack = 0
#     n_attack = 0

#     for row in rows:
#         element = row[6].strip()
#         if element:
#             counts[element] += 1
#         try:
#             attack = int(row[2])
#             total_attack += attack
#             n_attack += 1
#         except:
#             pass

#     avg_attack = total_attack / n_attack if n_attack > 0 else 0
#     return counts, avg_attack

# def hive_insert(counts):
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
#     return conn, cur

# # -----------------------------
# # Endpoint Prometheus
# # -----------------------------
# @app.route("/metrics")
# def metrics():
#     rows = read_csv()
#     counts, avg_attack = map_reduce(rows)
#     total_weapons = sum(counts.values())
    
#     response = f"pipeline_average_attack {avg_attack}\n"
#     response += f"pipeline_total_weapons {total_weapons}\n"
    
#     # Expose par élément
#     for elem, total in counts.items():
#         response += f"pipeline_weapon_count{{element=\"{elem}\"}} {total}\n"

#     return Response(response, mimetype="text/plain")

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)










# import csv
# import sqlite3
# from collections import defaultdict
# from flask import Flask, Response

# INPUT_CSV = "/home/mathys/Infra-et-orch-de-donn-es/Hadoop/data/weapons.csv"
# DB_NAME = "fake_hive.db"

# app = Flask(__name__)

# def read_csv():
#     rows = []
#     with open(INPUT_CSV, "r", encoding="utf-8") as f:
#         reader = csv.reader(f)
#         next(reader)
#         for line in reader:
#             if len(line) < 7:
#                 continue
#             rows.append(line)
#     return rows

# def map_reduce(rows):
#     counts = defaultdict(int)
#     for row in rows:
#         element = row[6]
#         if element.strip():
#             counts[element] += 1
#     return counts

# def generate_metrics():
#     rows = read_csv()
#     counts = map_reduce(rows)
#     total_weapons = sum(counts.values())
#     avg_attack = total_weapons / len(counts) if counts else 0

#     metrics = [
#         f"pipeline_total_weapons {total_weapons}",
#         f"pipeline_average_attack {avg_attack}"
#     ]
#     for elem, total in counts.items():
#         metrics.append(f'pipeline_weapon_count{{element="{elem}"}} {total}')
#     return "\n".join(metrics) + "\n"

# @app.route("/metrics")
# def metrics():
#     return Response(generate_metrics(), mimetype="text/plain")

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)
