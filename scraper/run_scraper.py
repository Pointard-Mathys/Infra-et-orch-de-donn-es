from scraper import sword_master, gunner, bows
import pandas as pd
from kafka import KafkaProducer
import json



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
# Attente/reconnexion Kafka
def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connecté à Kafka")
            return producer
        except Exception as e:
            print("⚠️ Kafka non disponible, nouvelle tentative dans 3s...", e)

producer = create_producer()



# ================================
# Fonction principale
# ================================
def run_all_scrapers():
    all_data = []
    total_weapons = 0

    for weapon_name, urls in URLS.items():
        print(f"\n=== Scraping {weapon_name} ===")

        if weapon_name in ["Light_Bowgun", "Heavy_Bowgun"]:
            scraper_func = gunner
        elif weapon_name == "Bows":
            scraper_func = bows
        else:
            scraper_func = sword_master

        for url in urls:
            data = scraper_func(url, weapon_name)
            all_data.extend(data)
            total_weapons += len(data)

            # ================================
            # Envoyer chaque arme à Kafka
            # ================================
            for weapon in data:
                producer.send('jeux', value=weapon)
            producer.flush()

    print(f"\n✅ Total d’armes collectées : {total_weapons}")
    return all_data

# ================================
# Lancement
# ================================
if __name__ == "__main__":
    results = run_all_scrapers()
    df = pd.DataFrame(results)
    print("\nAperçu du dataset complet :")
    print(df.head())

