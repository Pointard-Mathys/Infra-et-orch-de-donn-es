import requests
from bs4 import BeautifulSoup
import pandas as pd

# ================================
# Fonction principale de scraping
# ================================
def scrape_weapons(url, weapon_type, columns_specific):
    """Scrape une page d'armes MHFZ et retourne une liste de dicts."""
    print(f"\n=== Scraping {weapon_type} ({url}) ===")
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table')
    if not table:
        print("Aucune table trouvée sur la page.")
        return []

    rows = table.find_all('tr')
    results = []

    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue

        # Gestion des upgrades/disgrades
        divs = row.find_all('div')
        disgrade, upgrade = "", ""
        for div in divs:
            a_tags = div.find_all('a')
            if len(a_tags) == 1:
                upgrade = a_tags[0].get_text(strip=True).lower()
            elif len(a_tags) > 1:
                disgrade = a_tags[0].get_text(strip=True).lower()
                upgrade = "/".join([a.get_text(strip=True).lower() for a in a_tags[1:]])

        # Colonnes communes
        name = cols[0].get_text(strip=True).lower()
        rarity = cols[1].get_text(strip=True).lower()
        attributes = cols[2].get_text(strip=True).lower()

        # Extraction attaque / affinité / élément
        try:
            attack = attributes.split(" ")[0]
            affinity = attributes.split(" ")[1].split("%")[0] + "%"
            element = attributes.split("%")[1].strip()
        except Exception:
            attack, affinity, element = attributes, "", ""

        # Colonnes spécifiques
        extracted = {}
        for i, col_name in enumerate(columns_specific, start=3):
            if i < len(cols):
                extracted[col_name] = cols[i].get_text(strip=True).lower()
            else:
                extracted[col_name] = ""

        result = {
            "name": name,
            "disgrade": disgrade,
            "upgrade": upgrade,
            "rarity": rarity,
            "attack": attack,
            "affinity": affinity,
            "element": element,
        }
        result.update(extracted)
        results.append(result)

    print(f"{len(results)} armes trouvées pour {weapon_type}")
    return results


# ================================
# Fonctions pour chaque type d’arme
# ================================
def sword_master(url, weapon_type):
    columns = ["sharpness", "slots", "rank", "price", "creation_mats", "upgrade_mats"]
    return scrape_weapons(url, weapon_type, columns)

def gunner(url, weapon_type):
    columns = ["reload_recoil_bullet_speed", "ammo", "slots", "rank", "price", "creation_mats", "upgrade_mats"]
    return scrape_weapons(url, weapon_type, columns)

def bows(url, weapon_type):
    columns = ["charge_stage", "coatings", "slots", "rank", "price", "creation_mats", "upgrade_mats"]
    return scrape_weapons(url, weapon_type, columns)
