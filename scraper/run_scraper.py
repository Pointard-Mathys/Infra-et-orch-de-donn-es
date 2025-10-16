from scraper import sword_master, gunner, bows
from kafka import KafkaProducer
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock




# ================================
# URLs centralisées
# ================================
URLS = {
    "Long_Sword": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/tachi.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/tachi_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/tachi_s.htm#t5",
    ]
    # "Great_Sword": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/taiken.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/taiken_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/taiken_s.htm#t5",
    # ],
    # "Sword_and_Shield": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/katate.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/katate_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/katate_s.htm#t5",
    # ],
    # "Dual_Swords": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/souken.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/souken_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/souken_s.htm#t5",
    # ],
    # "Hammer": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/hammer.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/hammer_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/hammer_s.htm#t5",
    # ],
    # "Lance": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/lance.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/lance_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/lance_s.htm#t5",
    # ],
    # "Switch_axe": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/slaxe.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/slaxe_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/slaxe_s.htm#t5",
    # ],
    # "Bows": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/yumi.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/yumi_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/yumi_s.htm#t5",
    # ],
    # "Light_Bowgun": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/right.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/right_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/right_s.htm#t5",
    # ],
    # "Heavy_Bowgun": [
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/heavy.htm",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/heavy_g.htm#t8",
    #     "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/heavy_s.htm#t5",
    # ],
}






# ================================
# Connexion au broker Kafka
# ================================
def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connecté à Kafka")
            return producer
        except Exception as e:
            print("Kafka non disponible, nouvelle tentative dans 3s...", e)

producer = create_producer()

# ================================
# Configuration du chunk
# ================================

CHUNK_SIZE = 10000
chunk_lock = Lock()  # Lock pour sécuriser l'accès au chunk global
chunk = []

executor_kafka = ThreadPoolExecutor(max_workers=4)  # Pour l'envoi des chunks

def process_chunk_parallel(chunk_to_send):
    print(f"Traitement d'un chunk de {len(chunk_to_send)} armes")
    for weapon in chunk_to_send:
        producer.send('jeux', value=weapon)
    producer.flush()




# ================================
# Fonction de scraping thread-safe
# ================================

def scrape_and_chunk(scraper_func, url, weapon_name):
    global chunk
    data = scraper_func(url, weapon_name)
    with chunk_lock:
        for weapon in data:
            chunk.append(weapon)
            if len(chunk) >= CHUNK_SIZE:
                # Copier et envoyer en parallèle
                executor_kafka.submit(process_chunk_parallel, chunk.copy())
                chunk = []




# ================================
# Fonction principale
# ================================

def run_all_scrapers_parallel():
    total_weapons = 0
    futures = []

    with ThreadPoolExecutor(max_workers=4) as scrape_executor: 
        for weapon_name, urls in URLS.items():
            if weapon_name in ["Light_Bowgun", "Heavy_Bowgun"]:
                scraper_func = gunner
            elif weapon_name == "Bows":
                scraper_func = bows
            else:
                scraper_func = sword_master

            for url in urls:
                futures.append(scrape_executor.submit(scrape_and_chunk, scraper_func, url, weapon_name))

        for future in as_completed(futures):
            total_weapons += len(future.result() or [])

    # Traiter le dernier chunk restant
    with chunk_lock:
        if chunk:
            executor_kafka.submit(process_chunk_parallel, chunk.copy())
            chunk.clear()

    executor_kafka.shutdown(wait=True)
    print(f"\n✅ Total d’armes collectées : {total_weapons}")

# ================================
# Lancement
# ================================
if __name__ == "__main__":
    run_all_scrapers_parallel()
