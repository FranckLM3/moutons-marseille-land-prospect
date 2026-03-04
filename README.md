# 🐑 Moutons Marseillais — Land Prospect

> Carte interactive des terrains pâturables disponibles dans la métropole
> Aix-Marseille-Provence pour le projet de pastoralisme urbain.

[![GitHub Pages](https://img.shields.io/badge/carte-GitHub%20Pages-4ade80?style=flat-square&logo=github)](https://montons-marseillais.github.io/moutons-marseille-land-prospect/)
[![License: MIT](https://img.shields.io/badge/licence-MIT-blue?style=flat-square)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3572A5?style=flat-square&logo=python)](https://python.org)

---

## 🗺️ Aperçu

La carte identifie automatiquement les zones pâturables gérées par des personnes morales
(entreprises, collectivités…) à partir de deux sources de données ouvertes :

| Source | Description |
|--------|-------------|
| **OCS GE v2 — D13 2023** | Occupation du sol grande échelle (IGN), précision 1-2 m, format GeoPackage |
| **Parcelles des personnes morales** | OpenData Koumoul — propriétaires fonciers département 13 |

Les zones retenues couvrent les codes couverture `CS2.2.1` (formations herbacées) et
`CS2.1.2` (landes/maquis), avec une surface minimale configurable (défaut : 0,5 ha).

### Fonctionnalités de la carte

- 🗂️ **Filtres** — surface pâturable min, sélection des communes
- �� **Itinéraire** — calcul d'un trajet pédestre A→B via [OpenRouteService](https://openrouteservice.org/) (`foot-hiking`), avec affichage des parcelles candidates à portée du trajet et ajout d'un clic
- 📥 **Export CSV** des parcelles filtrées

---

## 📁 Structure

```
.
├── src/                        # Modules Python du pipeline géospatial
│   ├── land_cover.py           # Chargement & filtrage OCS GE
│   ├── filters.py              # Filtrage par surface
│   ├── cadastre.py             # Cadastre + calcul % prairie
│   ├── owners.py               # Chargement & jointure propriétaires
│   └── export.py               # Simplification géométrique & export GeoJSON
├── scripts/
│   └── build.py                # Pipeline principal (CLI)
├── docs/                       # Site GitHub Pages (déployé automatiquement)
│   ├── index.template.html     # Template HTML (clé ORS = __ORS_KEY__)
│   ├── app.js                  # Logique Leaflet / itinéraire
│   ├── app.css                 # Styles de l'interface
│   ├── .nojekyll               # Désactive Jekyll sur GitHub Pages
│   └── pasture_zones.geojson   # Données générées (versionnées)
├── data/                       # Données brutes (non versionnées)
│   ├── ocsge/                  # Déposer le .gpkg ici
│   ├── cadastre/               # Parcelles cadastrales (téléchargées automatiquement)
│   └── parcelles-des-personnes-morales.geojson
├── .github/workflows/
│   └── deploy.yml              # CI/CD → GitHub Pages (injecte ORS_API_KEY secret)
└── archive/                    # Anciens scripts (référence)
```

> **`docs/index.html` est ignoré par git** — généré localement par `build.py`
> (contient la clé ORS en clair) et en CI par le workflow via le secret `ORS_API_KEY`.

---

## ⚙️ Installation

### 1. Cloner le dépôt

```bash
git clone https://gitea.evolix.org/donut-marseille/moutons-marseille-land-prospect.git
cd moutons-marseille-land-prospect
```

### 2. Installer les dépendances

Avec **uv** (recommandé) :

```bash
uv venv && uv pip install -r requirements.txt
```

Ou avec **pip** :

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configurer la clé ORS

Créer un fichier `.env` à la racine (jamais commité) :

```bash
echo "ORS_API_KEY=votre_clé_ici" > .env
```

Obtenir une clé gratuite sur [openrouteservice.org](https://openrouteservice.org/dev/#/signup) (2 000 req/jour).

### 3b. (Optionnel) Activer les avis contributeurs via Supabase

Créer un projet Supabase, puis ajouter ces tables :

```sql
create table if not exists parcel_feedback (
      parcel_id text primary key,
      status text default 'unknown',
      updated_at timestamptz default now()
);

create table if not exists parcel_comments (
      id uuid primary key default gen_random_uuid(),
      parcel_id text not null,
      author text default 'Anonyme',
      message text not null,
      created_at timestamptz default now()
);

-- (Optionnel) RLS simple pour autoriser lecture/écriture publique
alter table parcel_feedback enable row level security;
alter table parcel_comments enable row level security;

create policy "public read feedback" on parcel_feedback
      for select using (true);

create policy "public upsert feedback" on parcel_feedback
      for insert with check (true);

create policy "public update feedback" on parcel_feedback
      for update using (true);

create policy "public read comments" on parcel_comments
      for select using (true);

create policy "public insert comments" on parcel_comments
      for insert with check (true);
```

Puis ajouter dans `.env` :

```bash
SUPABASE_URL=https://<votre-projet>.supabase.co
SUPABASE_ANON_KEY=<votre-anon-key>
```

Sans ces variables, la carte utilise uniquement le stockage local du navigateur.

### 4. Télécharger les données OCS GE D13

```bash
mkdir -p data/ocsge
curl -L "https://data.geopf.fr/telechargement/download/OCSGE/OCS-GE_2-0__GPKG_LAMB93_D013_2023-01-01/OCS-GE_2-0__GPKG_LAMB93_D013_2023-01-01.7z" \
     -o data/ocsge/ocsge_d13.7z
7z x data/ocsge/ocsge_d13.7z -odata/ocsge/
```

---

## 🚀 Utilisation

### Pipeline complet (données + carte)

```bash
python scripts/build.py
```

Options disponibles :

```
--gpkg PATH         GeoPackage OCS GE (défaut: data/ocsge/…)
--owners PATH       GeoJSON propriétaires
--output PATH       Sortie (défaut: docs/pasture_zones.geojson)
--min-area M2       Surface minimale en m² (défaut: 5000)
--max-area M2       Surface maximale en m² (optionnel)
--min-prairie PCT   % prairie minimum 0–100 (défaut: 0)
--include-without-owner  Inclure les parcelles sans propriétaire
--inject-only       Génère uniquement docs/index.html depuis le template
```

### Régénérer uniquement la carte (après modif de app.js / app.css)

```bash
python scripts/build.py --inject-only
```

### Visualiser en local

```bash
python -m http.server 8000 --directory docs --bind 127.0.0.1
# ouvrir http://127.0.0.1:8000
```

---

## 🧪 Pipeline de traitement

```
[1/5] load_ocsge()              → charge le GeoPackage, reprojecte en EPSG:4326
[2/5] filter_pasture_zones()    → filtre par code couverture CS (herbacé, landes…)
[3/5] load_cadastre()           → parcelles cadastrales Etalab
      add_area_columns()         → area_m2 et area_ha
      filter_by_area()           → seuil surface
      load_owners()              → propriétaires fonciers
      join_owners_to_cadastre()  → jointure sur numéro de parcelle
[4/5] add_prairie_ratio()       → intersection OCS GE × cadastre → pct_prairie
[5/5] prepare_for_export()      → simplification géométrique (2 m)
      export_geojson()           → docs/pasture_zones.geojson
      _inject_ors_key()          → docs/index.html depuis template + clé ORS
```

---

## 🚢 Déploiement

### Flux : Gitea → GitHub → GitHub Pages

```
code local  →  push Gitea  →  push GitHub  →  GitHub Actions  →  GitHub Pages
```

**1. Configurer le secret GitHub** dans *Settings → Secrets and variables → Actions* :
```
ORS_API_KEY = <votre clé>
SUPABASE_URL = <votre url>
SUPABASE_ANON_KEY = <votre anon key>
```

**2. Ajouter le remote GitHub** (une seule fois) :
```bash
git remote add github https://github.com/montons-marseillais/moutons-marseille-land-prospect.git
```

**3. Pusher** :
```bash
git push origin main   # Gitea
git push github main   # GitHub → déclenche le déploiement automatique
```

Le workflow `.github/workflows/deploy.yml` génère `docs/index.html` depuis le template
avec la clé ORS injectée, puis déploie `docs/` sur GitHub Pages.

---

## 📊 Sources de données

| Jeu de données | Licence | Lien |
|---|---|---|
| OCS GE IGN v2 D13 2023 | Licence Ouverte Etalab | [geoservices.ign.fr](https://geoservices.ign.fr/ocsge) |
| Cadastre Etalab | Licence Ouverte Etalab | [cadastre.data.gouv.fr](https://cadastre.data.gouv.fr) |
| Parcelles personnes morales | Licence Ouverte | [koumoul.com](https://koumoul.com/datasets/parcelles-des-personnes-morales) |

---

## 📄 Licence

[MIT](LICENSE) © Moutons Marseillais
