# Moutons Marseille — Prospection Terrains

Outil d'analyse géospatiale pour identifier des terrains adaptés au pâturage ovin dans la métropole marseillaise.

## Vue d'ensemble

Ce projet croise les données MOS (Mode d'Occupation du Sol) 2022 avec les informations de propriété foncière pour produire une interface cartographique interactive permettant de visualiser et filtrer les terrains potentiels.

## Installation

1. Cloner le repository et créer un environnement virtuel :
```bash
git clone <repo-url>
cd moutons-marseille-land-prospect
python3 -m venv .venv
source .venv/bin/activate  # ou .venv/bin/activate sur macOS/Linux
```

2. Installer les dépendances :
```bash
pip install geopandas pandas shapely pyogrio
```

## Utilisation

### Traitement des données
```bash
.venv/bin/python process.py
```
Cette commande :
- Charge le shapefile MOS depuis `data/MOS_2022/`
- Filtre les parcelles (surfaces >5000m², classifications spécifiques)
- Effectue une jointure spatiale avec les données de propriété
- Génère `front/mos_2022_occsol_filtered.geojson` (48 477 features)

### Interface web
```bash
python3 -m http.server 8000 --directory front --bind 127.0.0.1
```
Ouvrir http://127.0.0.1:8000 dans le navigateur.

L'interface permet :
- Filtrage par type d'occupation du sol (niveaux 1 et 2)
- Sélection par commune
- Visualisation des informations de propriété au clic

## Sources de données

- **MOS 2022** : [Mode d'occupation du sol - Métropole Aix-Marseille-Provence](https://data.ampmetropole.fr/explore/dataset/ol-integration-mos/table/)
- **Parcelles propriétaires** : [Koumoul OpenData - Parcelles des personnes morales](https://opendata.koumoul.com/datasets/parcelles-des-personnes-morales/full?p=%2Fdata-fair%2Fembed%2Fdataset%2Fparcelles-des-personnes-morales%2Ftable&departement_eq=13&cols=numero_siren,denomination,_infos_commune.code_commune,_infos_commune.nom_commune,_parcelle_coords.coord,adresse) (département 13)

## Structure

```
├── process.py              # Script de traitement principal
├── data/
│   ├── MOS_2022/          # Données source MOS (shapefile)
│   └── parcelles-des-personnes-morales.geojson  # Données propriétaires
├── front/
│   ├── index.html         # Interface Leaflet
│   └── mos_2022_occsol_filtered.geojson  # Données filtrées (générées)
└── archive/               # Scripts et données historiques
```
