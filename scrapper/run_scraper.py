from scraper import sword_master, gunner, bows
import pandas as pd

# ================================
# URLs centralisées
# ================================
URLS = {
    "Long_Sword": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/tachi.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/tachi_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/tachi_s.htm#t5",
    ],
    "Great_Sword": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/taiken.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/taiken_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/taiken_s.htm#t5",
    ],
    "Sword_and_Shield": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/katate.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/katate_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/katate_s.htm#t5",
    ],
    "Dual_Swords": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/souken.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/souken_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/souken_s.htm#t5",
    ],
    "Hammer": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/hammer.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/hammer_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/hammer_s.htm#t5",
    ],
    "Lance": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/lance.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/lance_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/lance_s.htm#t5",
    ],
    "Switch_axe": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/slaxe.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/slaxe_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/slaxe_s.htm#t5",
    ],
    "Bows": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/yumi.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/yumi_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/yumi_s.htm#t5",
    ],
    "Light_Bowgun": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/right.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/right_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/right_s.htm#t5",
    ],
    "Heavy_Bowgun": [
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/heavy.htm",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/heavy_g.htm#t8",
        "https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/heavy_s.htm#t5",
    ],
}

# ================================
# Fonction principale
# ================================
def run_all_scrapers():
    all_data = []
    total_weapons = 0

    for weapon_name, urls in URLS.items():
        print(f"\n=== Scraping {weapon_name} ===")

        if weapon_name in ["Light_Bowgun", "Heavy_Bowgun"]:
            scraper = gunner
        elif weapon_name == "Bows":
            scraper = bows
        else:
            scraper = sword_master

        for url in urls:
            data = scraper(url, weapon_name)
            all_data.extend(data)
            total_weapons += len(data)

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

    df.to_csv("mhfz_full_dataset.csv", index=False)
    print("\n💾 Fichier enregistré : mhfz_full_dataset.csv")
