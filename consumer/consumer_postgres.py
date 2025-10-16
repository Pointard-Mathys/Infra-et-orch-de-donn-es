from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_batch
import time
import sys

# ================================
# Config Kafka
# ================================
TOPIC = "jeux"
BOOTSTRAP_SERVERS = 'kafka:9092'

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("⏳ Consumer connecté à Kafka...")
            return consumer
        except Exception as e:
            print("⚠️ Erreur Kafka, nouvelle tentative dans 5s :", e)
            time.sleep(5)

consumer = create_consumer()

# ================================
# Config PostgreSQL
# ================================
PG_HOST = 'postgres'
PG_PORT = 5432
PG_DB = 'postgresdb'
PG_USER = 'postgresuser'
PG_PASSWORD = 'postgrespassword'

try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()
    print("✅ Connexion PostgreSQL OK")
except Exception as e:
    print("❌ Erreur de connexion PostgreSQL :", e)
    sys.exit(1)

# ================================
# Création table
# ================================
try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weapons (
        name TEXT,
        disgrade TEXT,
        upgrade TEXT,
        rarity TEXT,
        attack TEXT,
        affinity TEXT,
        element TEXT,
        sharpness TEXT,
        slots TEXT,
        rank TEXT,
        price TEXT,
        creation_mats TEXT,
        upgrade_mats TEXT,
        description TEXT
    )
    """)
    conn.commit()
    print("✅ Table PostgreSQL prête")
except Exception as e:
    print("❌ Erreur création table :", e)
    conn.rollback()
    sys.exit(1)

# ================================
# Lire depuis Kafka et insérer
# ================================
batch = []
BATCH_SIZE = 500

try:
    for message in consumer:
        try:
            batch.append(message.value)
            if len(batch) >= BATCH_SIZE:
                keys = batch[0].keys()
                cols = ','.join(keys)
                vals = [[record.get(k) for k in keys] for record in batch]
                execute_batch(
                    cursor,
                    f"INSERT INTO weapons ({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    vals
                )
                conn.commit()
                print(f"📦 {len(batch)} lignes insérées dans PostgreSQL")
                batch = []

        except Exception as e:
            print("⚠️ Erreur insertion batch :", e)
            conn.rollback()
            batch = []

except KeyboardInterrupt:
    print("\n⚠️ Interruption utilisateur")

finally:
    if batch:
        try:
            keys = batch[0].keys()
            cols = ','.join(keys)
            vals = [[record.get(k) for k in keys] for record in batch]
            execute_batch(
                cursor,
                f"INSERT INTO weapons ({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                vals
            )
            conn.commit()
            print(f"📦 {len(batch)} lignes insérées dans PostgreSQL (reste)")
        except Exception as e:
            print("❌ Erreur insertion finale :", e)
            conn.rollback()

    cursor.close()
    conn.close()
    consumer.close()
    print("✅ Toutes les données ont été insérées et les connexions fermées !")
