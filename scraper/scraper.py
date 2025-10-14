import requests
from bs4 import BeautifulSoup


def sword_master(url, name_table):   
    print(f"=== Scrapping Sword Master : {name_table} ===")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table')
    rows = table.find_all('tr')

    results = []

    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue

        divs = row.find_all('div')
        Disgrade = ""
        Upgrade = ""
        for div in divs:
            a_tags = div.find_all('a')
            if len(a_tags) == 1:
                Upgrade = a_tags[0].get_text().lower()
                Disgrade = None
            elif len(a_tags) > 1:
                Disgrade = a_tags[0].get_text().lower()
                for a_tag in a_tags[1:]:
                    Upgrade += a_tag.get_text().lower() + "/"

        # Nettoyage des balises <a>
        for s in div('a'):
            if "←" in s.get_text():
                s.extract()

        # Extraction des colonnes
        name = cols[0].get_text().strip().lower()
        rarity = cols[1].get_text().strip().lower()
        attributes = cols[2].get_text().strip().lower()

        try:
            att_att = attributes.split(" ")[0]
            att_tab_aff = attributes.split(" ")
            att_tab2_aff = att_tab_aff[1].split("%")
            att_aff = att_tab2_aff[0] + "%"
            attribute = attributes.split("%")[1]
        except Exception:
            att_att, att_aff, attribute = attributes, "", ""

        sharpness = cols[3].get_text().strip().lower()
        slots = cols[4].get_text().strip().lower()
        rank = cols[5].get_text().strip().lower()
        price = cols[6].get_text().strip().lower()
        creation_mats = cols[7].get_text().lower()
        upgrade_mats = cols[8].get_text().lower()
        description = cols[9].text.strip()

        results.append({
            "name": name,
            "disgrade": Disgrade,
            "upgrade": Upgrade,
            "rarity": rarity,
            "attack": att_att,
            "attributes": attribute,
            "affinity": att_aff,
            "sharpness": sharpness,
            "slots": slots,
            "rank": rank,
            "price": price,
            "creation_mats": creation_mats,
            "upgrade_mats": upgrade_mats,
            "description": description
        })

    print(f"Nombre d’armes trouvées : {len(results)}")
    for r in results:
        print(r)

    return results



def gunner(url, name_table):   
    print(f"=== Scrapping Gunner : {name_table} ===")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table')
    rows = table.find_all('tr')

    results = []

    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue

        divs = row.find_all('div')
        Disgrade = ""
        Upgrade = ""
        for div in divs:
            a_tags = div.find_all('a')
            if len(a_tags) == 1:
                Upgrade = a_tags[0].get_text().lower()
                Disgrade = None
            elif len(a_tags) > 1:
                Disgrade = a_tags[0].get_text().lower()
                for a_tag in a_tags[1:]:
                    Upgrade += a_tag.get_text().lower() + "/"

            for s in div('a'):
                if "←" in s.get_text():
                    s.extract()

        name = cols[0].get_text().strip().lower()
        rarity = cols[1].get_text().strip().lower()
        attributes = cols[2].get_text().strip().lower()

        try:
            att_att = attributes.split(" ")[0]
            att_tab_aff = attributes.split(" ")
            att_tab2_aff = att_tab_aff[1].split("%")
            att_aff = att_tab2_aff[0] + "%"
            attribute = attributes.split("%")[1]
        except Exception:
            att_att, att_aff, attribute = attributes, "", ""

        Reload_Recoil_Bullet_Speed = cols[3].get_text().strip().lower()
        ammo = cols[4].get_text().strip().lower()
        slots = cols[5].get_text().strip().lower()
        rank = cols[6].get_text().strip().lower()
        price = cols[7].get_text().lower()
        creation_mats = cols[8].get_text().lower()
        upgrade_mats = cols[9].text.strip().lower()
        description = cols[10].text.strip()

        results.append({
            "name": name,
            "previous_level": Disgrade,
            "upgrade": Upgrade,
            "rarity": rarity,
            "attack": att_att,
            "attributes": attribute,
            "affinity": att_aff,
            "reload_recoil_bullet_speed": Reload_Recoil_Bullet_Speed,
            "ammo": ammo,
            "slots": slots,
            "rank": rank,
            "price": price,
            "creation_mats": creation_mats,
            "upgrade_mats": upgrade_mats,
            "description": description
        })

    print(f"Nombre d’armes trouvées : {len(results)}")
    for r in results:
        print(r)

    return results



def bows(url, name_table):   
    print(f"=== Scrapping Bows : {name_table} ===")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table')
    rows = table.find_all('tr')

    results = []

    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue

        divs = row.find_all('div')
        Disgrade = ""
        Upgrade = ""
        for div in divs:
            a_tags = div.find_all('a')
            if len(a_tags) == 1:
                Upgrade = a_tags[0].get_text().lower()
                Disgrade = None
            elif len(a_tags) > 1:
                Disgrade = a_tags[0].get_text().lower()
                for a_tag in a_tags[1:]:
                    Upgrade += a_tag.get_text().lower() + "/"

            for s in div('a'):
                if "←" in s.get_text():
                    s.extract()

        name = cols[0].get_text().strip().lower()
        rarity = cols[1].get_text().strip().lower()
        attributes = cols[2].get_text().strip().lower()

        try:
            att_att = attributes.split(" ")[0]
            att_tab_aff = attributes.split(" ")
            att_tab2_aff = att_tab_aff[1].split("%")
            att_aff = att_tab2_aff[0] + "%"
            attribute = attributes.split("%")[1]
        except Exception:
            att_att, att_aff, attribute = attributes, "", ""

        charge_stage = cols[3].get_text().strip().lower()
        coatings = cols[4].get_text().strip().lower()
        slots = cols[5].get_text().strip().lower()
        rank = cols[6].get_text().strip().lower()
        price = cols[7].get_text().lower()
        creation_mats = cols[8].get_text().lower()
        upgrade_mats = cols[9].text.strip().lower()
        description = cols[10].text.strip()

        results.append({
            "name": name,
            "previous_level": Disgrade,
            "upgrade": Upgrade,
            "rarity": rarity,
            "attack": att_att,
            "attributes": attribute,
            "affinity": att_aff,
            "charge_stage": charge_stage,
            "coatings": coatings,
            "slots": slots,
            "rank": rank,
            "price": price,
            "creation_mats": creation_mats,
            "upgrade_mats": upgrade_mats,
            "description": description
        })

    print(f"Nombre d’armes trouvées : {len(results)}")
    for r in results:
        print(r)

    return results


sword_master("https://xl3lackout.github.io/MHFZ-Ferias-English-Project/buki/tachi.htm", "Long_Sword")
