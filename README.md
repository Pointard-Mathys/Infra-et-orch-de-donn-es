### Infra-et-orch-de-donn-es
## Objectif du projet

L’objectif de ce projet est de mettre en place une infrastructure de traitement de données en continu à l’aide de Kafka et PostgreSQL, afin d’automatiser la collecte, la transmission et le stockage des données issues du site Monster Hunter Frontier Z (MHFZ).

Le pipeline repose sur trois composants principaux :

 # Scraper Python (Producteur Kafka)

Scrape automatiquement les données d’armes depuis le site MHFZ.

Transforme les informations collectées (nom, rareté, attaque, affinité, etc.) en objets JSON.

Envoie ces messages dans un topic Kafka dédié.

 # Kafka Broker

Sert d’intermédiaire entre le producteur et le consommateur.

Gère la file de messages entrants et sortants.

Stocke temporairement les messages envoyés par le producteur avant qu’ils ne soient lus par le consommateur.

Cette étape intermédiaire permet d’éviter toute perte de données en cas de surcharge ou de panne, et de traiter les messages à un rythme adapté à la base de données.

 # PostgreSQL (Stockage persistant)

Reçoit les messages consommés depuis Kafka.

Stocke les données de manière structurée dans des tables PostgreSQL, une par type d’arme (ex. long_sword, lance, bow, etc.).

Permet de conserver un historique complet et de faciliter les analyses ultérieures.

L’ensemble du système est conteneurisé avec Docker Compose, afin d’assurer :

    - Un déploiement simple et reproductible sur n’importe quel environnement.

    - Une isolation des composants (Kafka, Scraper, PostgreSQL).

    - Une automatisation complète du pipeline de données.

## Instructions pour lancer le stack

Il faut commencer par cloner le projet.
Ensuite on l'exécute avec la commande :
    docker compose up --build

Cela va :

Lancer un container Kafka (broker unique).

Lancer le scraper Python qui agit comme producteur Kafka.

Démarrer le service PostgreSQL.

Connecter les trois services sur un réseau interne Docker.

## Comment produire et consommer des messages

Le producer Kafka est directement dans run_scraper.py.
Lors de l’exécution du service scraper, le script :

Scrape les pages MHFZ.

Transforme chaque arme en message JSON.

Publie ce message dans le topic Kafka (weapon_data).


Pour le consumer, on a un script séparé consumer.py :

Se connecte au même broker Kafka.

Écoute le topic weapon_data.

Récupère les messages JSON.

Insère chaque message dans la base PostgreSQL.

