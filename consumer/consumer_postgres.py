from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_batch


# Config Kafka
<<<<<<< HEAD
# ================================
TOPIC = "scraper-topic"
=======

TOPIC = "jeux"
>>>>>>> bedeaab0c0db3f9725d882cc4ce26bf0e48ec3ef
BOOTSTRAP_SERVERS = 'kafka:9092'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',  # lire depuis le début
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("⏳ Consumer connecté à Kafka...")


# Config PostgreSQL
<<<<<<< HEAD
# ================================
PG_HOST = 'postgres'  # nom du container postgres dans docker-compose
=======

PG_HOST = 'postgres'
>>>>>>> bedeaab0c0db3f9725d882cc4ce26bf0e48ec3ef
PG_PORT = 5432
PG_DB = 'postgresdb'
PG_USER = 'postgresuser'
PG_PASSWORD = 'postgrespassword'

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
cursor = conn.cursor()

<<<<<<< HEAD
# Crée la table si elle n'existe pas
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

print("✅ PostgreSQL prêt")
=======

# Création table

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
        upgrade_mats TEXT
    )
    """)
    conn.commit()
    print(" Table PostgreSQL prête")
except Exception as e:
    print(" Erreur création table :", e)
    conn.rollback()
    sys.exit(1)
>>>>>>> bedeaab0c0db3f9725d882cc4ce26bf0e48ec3ef


# Lire depuis Kafka et insérer

batch = []
BATCH_SIZE = 500  # insère par lot pour gagner en performance

for message in consumer:
    batch.append(message.value)
    if len(batch) >= BATCH_SIZE:
        # Insertion batch
        keys = batch[0].keys()
        cols = ','.join(keys)
        vals = [[record[k] for k in keys] for record in batch]
        execute_batch(
            cursor,
            f"INSERT INTO weapons ({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            vals
        )
        conn.commit()
        print(f"📦 {len(batch)} lignes insérées dans PostgreSQL")
        batch = []

# Insérer le reste
if batch:
    keys = batch[0].keys()
    cols = ','.join(keys)
    vals = [[record[k] for k in keys] for record in batch]
    execute_batch(
        cursor,
        f"INSERT INTO weapons ({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        vals
    )
    conn.commit()
    print(f"📦 {len(batch)} lignes insérées dans PostgreSQL (reste)")

cursor.close()
conn.close()
consumer.close()
print("✅ Toutes les données ont été insérées !")
