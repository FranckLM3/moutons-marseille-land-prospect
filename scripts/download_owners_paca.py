#!/usr/bin/env python3
"""
Télécharge les parcelles des personnes morales via l'API Koumoul
pour les départements PACA (04, 05, 06, 13, 83, 84) et les fusionne
en un fichier GeoJSON unique.

Le fichier produit remplace l'ancien parcelles-des-personnes-morales.geojson
(téléchargement national de 68 Mo) par une version régionale enrichie
avec le champ `groupe_personne` (→ proprietaire_type public/privé).

API utilisée :
  https://opendata.koumoul.com/data-fair/api/v1/datasets/parcelles-des-personnes-morales/lines
  Pagination : size=10000, after= (curseur _i)
  Filtre dept : qs=departement:<code>

Usage :
    python scripts/download_owners_paca.py
    python scripts/download_owners_paca.py --dept 83 84   # seulement Var + Vaucluse
    python scripts/download_owners_paca.py --all          # inclut le 13
    python scripts/download_owners_paca.py --output data/owners_paca.geojson
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

PACA_DEPTS = {
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "13": "Bouches-du-Rhône",
    "83": "Var",
    "84": "Vaucluse",
}

# API Koumoul — Data Fair
KOUMOUL_BASE = "https://opendata.koumoul.com/data-fair/api/v1/datasets/parcelles-des-personnes-morales"
PAGE_SIZE = 10_000

# Mapping groupe_personne → proprietaire_type (cohérent avec src/owners.py)
GROUPE_PERSONNE_TYPE: dict[int, str] = {
    0: "indéterminé",
    1: "public",
    2: "public",
    3: "public",
    4: "public",
    5: "semi-public",
    6: "semi-public",
    7: "privé",
    8: "privé",
    9: "privé",
}


def fetch_url(url: str) -> dict:
    """Télécharge une URL et retourne le JSON parsé."""
    req = urllib.request.Request(url, headers={"User-Agent": "moutons-marseille/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def first_page_url(dept: str) -> str:
    """Construit l'URL de la première page pour un département."""
    params: dict = {
        "size": PAGE_SIZE,
        "qs": f"departement:{dept}",
        "select": "code_parcelle,denomination,numero_siren,groupe_personne,nom_commune,departement,_geopoint",
    }
    return f"{KOUMOUL_BASE}/lines?{urllib.parse.urlencode(params)}"


def row_to_feature(row: dict) -> dict | None:
    """Convertit une ligne API en feature GeoJSON Point."""
    geopoint = row.get("_geopoint")
    if not geopoint:
        return None
    try:
        lat_str, lon_str = geopoint.split(",")
        lat, lon = float(lat_str.strip()), float(lon_str.strip())
    except (ValueError, AttributeError):
        return None

    groupe = row.get("groupe_personne")
    prop_type = GROUPE_PERSONNE_TYPE.get(int(groupe), "indéterminé") if groupe is not None else "indéterminé"

    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "code_parcelle":     row.get("code_parcelle"),
            "denomination":      row.get("denomination"),
            "numero_siren":      row.get("numero_siren"),
            "groupe_personne":   groupe,
            "proprietaire_type": prop_type,
            "nom_commune":       row.get("nom_commune"),
            "departement":       row.get("departement"),
        },
    }


def download_dept(dept: str) -> list[dict]:
    """Télécharge toutes les pages pour un département et retourne les features."""
    features: list[dict] = []

    url: str | None = first_page_url(dept)

    # Récupérer le total sur la première page
    first = fetch_url(url)
    total = first.get("total", 0)
    print(f"  → {total:,} parcelles trouvées pour le dept {dept}")

    for row in first.get("results", []):
        feat = row_to_feature(row)
        if feat:
            features.append(feat)

    # Pagination via l'URL `next` fournie par l'API
    url = first.get("next")

    while url:
        time.sleep(0.2)  # politesse
        data = fetch_url(url)
        results = data.get("results", [])
        for row in results:
            feat = row_to_feature(row)
            if feat:
                features.append(feat)
        if len(features) % 50_000 < PAGE_SIZE:
            print(f"    {len(features):,}/{total:,} téléchargées…")
        url = data.get("next") if results else None

    print(f"  ✅ Dept {dept} : {len(features):,} features")
    return features


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dept", nargs="+", choices=list(PACA_DEPTS.keys()),
                        help="Départements à télécharger (défaut: tous sauf 13)")
    parser.add_argument("--all", action="store_true",
                        help="Inclure le département 13 (BdR)")
    parser.add_argument("--output", default=str(ROOT / "data" / "parcelles-des-personnes-morales.geojson"),
                        help="Fichier GeoJSON de sortie (remplace l'existant)")
    return parser.parse_args()


def main():
    args = parse_args()
    depts = args.dept or ([k for k in PACA_DEPTS] if args.all else [k for k in PACA_DEPTS if k != "13"])

    print(f"\n🏛️  Téléchargement parcelles personnes morales — départements : {', '.join(depts)}")
    print("=" * 60)

    all_features: list[dict] = []

    for dept in depts:
        print(f"\n── Département {dept} — {PACA_DEPTS[dept]} ──")
        try:
            feats = download_dept(dept)
            all_features.extend(feats)
        except Exception as e:
            print(f"  ❌ Erreur dept {dept} : {e}")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    geojson = {
        "type": "FeatureCollection",
        "features": all_features,
    }
    output.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")

    print(f"\n{'=' * 60}")
    print(f"✅ {len(all_features):,} features exportées → {output}")
    print()
    print("📋 Prochaines étapes :")
    for dept in depts:
        print(f"  python scripts/build.py --gpkg <ocsge_{dept}.gpkg> --owners {output} --output data/pasture_{dept}.geojson")
    print(f"  python scripts/import_parcelles_supabase.py --geojson data/pasture_<dept>.geojson")


if __name__ == "__main__":
    main()
