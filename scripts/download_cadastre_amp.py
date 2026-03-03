#!/usr/bin/env python3
"""
Télécharge les parcelles cadastrales (Etalab) pour toutes les communes
de la Métropole Aix-Marseille-Provence (92 communes).

Source : https://cadastre.data.gouv.fr/datasets/cadastre-etalab
Fichiers : cadastre-<code>-parcelles.json.gz

Marseille (code INSEE 13055) est découpée en 16 arrondissements municipaux
(codes 13201 à 13216), chacun ayant son propre fichier cadastral.

Usage :
    python scripts/download_cadastre_amp.py [--output-dir data/cadastre]
"""

from __future__ import annotations

import argparse
import time
import urllib.request
from pathlib import Path

BASE_URL = "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/communes"

# Codes INSEE des communes AMP (hors Marseille qui est décomposée en arrondissements)
# Source : API geo.api.gouv.fr/epcis/200054807/communes
AMP_COMMUNE_CODES: list[str] = [
    "13001",  # Aix-en-Provence
    "13002",  # Allauch
    "13003",  # Alleins
    "13005",  # Aubagne
    "13007",  # Auriol
    "13008",  # Aurons
    "13009",  # La Barben
    "13012",  # Beaurecueil
    "13013",  # Belcodène
    "13014",  # Berre-l'Étang
    "13015",  # Bouc-Bel-Air
    "13016",  # La Bouilladisse
    "13019",  # Cabriès
    "13020",  # Cadolive
    "13021",  # Carry-le-Rouet
    "13022",  # Cassis
    "13023",  # Ceyreste
    "13024",  # Charleval
    "13025",  # Châteauneuf-le-Rouge
    "13026",  # Châteauneuf-les-Martigues
    "13028",  # La Ciotat
    "13029",  # Cornillon-Confoux
    "13030",  # Cuges-les-Pins
    "13031",  # La Destrousse
    "13032",  # Éguilles
    "13033",  # Ensuès-la-Redonne
    "13035",  # Eyguières
    "13037",  # La Fare-les-Oliviers
    "13039",  # Fos-sur-Mer
    "13040",  # Fuveau
    "13041",  # Gardanne
    "13042",  # Gémenos
    "13043",  # Gignac-la-Nerthe
    "13044",  # Grans
    "13046",  # Gréasque
    "13047",  # Istres
    "13048",  # Jouques
    "13049",  # Lamanon
    "13050",  # Lambesc
    "13051",  # Lançon-Provence
    "13053",  # Mallemort
    "13054",  # Marignane
    # 13055 = Marseille → voir MARSEILLE_CODES ci-dessous
    "13056",  # Martigues
    "13059",  # Meyrargues
    "13060",  # Meyreuil
    "13062",  # Mimet
    "13063",  # Miramas
    "13069",  # Pélissanne
    "13070",  # La Penne-sur-Huveaune
    "13071",  # Les Pennes-Mirabeau
    "13072",  # Peynier
    "13073",  # Peypin
    "13074",  # Peyrolles-en-Provence
    "13075",  # Plan-de-Cuques
    "13077",  # Port-de-Bouc
    "13078",  # Port-Saint-Louis-du-Rhône
    "13079",  # Puyloubier
    "13080",  # Le Puy-Sainte-Réparade
    "13081",  # Rognac
    "13082",  # Rognes
    "13084",  # La Roque-d'Anthéron
    "13085",  # Roquefort-la-Bédoule
    "13086",  # Roquevaire
    "13087",  # Rousset
    "13088",  # Le Rove
    "13090",  # Saint-Antonin-sur-Bayon
    "13091",  # Saint-Cannat
    "13092",  # Saint-Chamas
    "13093",  # Saint-Estève-Janson
    "13095",  # Saint-Marc-Jaumegarde
    "13098",  # Saint-Mitre-les-Remparts
    "13099",  # Saint-Paul-lès-Durance
    "13101",  # Saint-Savournin
    "13102",  # Saint-Victoret
    "13103",  # Salon-de-Provence
    "13104",  # Sausset-les-Pins
    "13105",  # Sénas
    "13106",  # Septèmes-les-Vallons
    "13107",  # Simiane-Collongue
    "13109",  # Le Tholonet
    "13110",  # Trets
    "13111",  # Vauvenargues
    "13112",  # Velaux
    "13113",  # Venelles
    "13114",  # Ventabren
    "13115",  # Vernègues
    "13117",  # Vitrolles
    "13118",  # Coudoux
    "13119",  # Carnoux-en-Provence
    "83120",  # Saint-Zacharie (Var)
    "84089",  # Pertuis (Vaucluse)
]

# Marseille est découpée en 16 arrondissements dans le cadastre Etalab
MARSEILLE_CODES: list[str] = [f"132{i:02d}" for i in range(1, 17)]


def get_dept(code: str) -> str:
    """Retourne le code département à 2 chiffres à partir du code INSEE."""
    if code.startswith("13"):
        return "13"
    elif code.startswith("83"):
        return "83"
    elif code.startswith("84"):
        return "84"
    return code[:2]


def download_file(url: str, dest: Path, skip_existing: bool = True) -> bool:
    """Télécharge un fichier. Retourne True si téléchargé, False si ignoré."""
    if skip_existing and dest.exists():
        print(f"  ✓ déjà présent : {dest.name}")
        return False

    print(f"  ↓ {dest.name} …", end=" ", flush=True)
    try:
        urllib.request.urlretrieve(url, dest)
        size_kb = dest.stat().st_size // 1024
        print(f"OK ({size_kb} KB)")
        return True
    except Exception as e:
        print(f"ERREUR : {e}")
        # Supprimer le fichier partiel si créé
        if dest.exists():
            dest.unlink()
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Télécharge les parcelles cadastrales AMP")
    parser.add_argument(
        "--output-dir",
        default="data/cadastre",
        help="Dossier de destination (défaut : data/cadastre)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Ne pas re-télécharger les fichiers déjà présents (défaut : True)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-télécharger même si le fichier existe déjà",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        help="Délai entre chaque requête en secondes (défaut : 0.3)",
    )
    return parser.parse_args()


def run() -> None:
    args = parse_args()
    skip = not args.force

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_codes = MARSEILLE_CODES + AMP_COMMUNE_CODES
    total = len(all_codes)
    downloaded = 0
    skipped = 0
    errors = 0

    print(f"Téléchargement des parcelles cadastrales pour {total} communes/arrondissements AMP")
    print(f"Destination : {output_dir.resolve()}\n")

    for i, code in enumerate(all_codes, 1):
        dept = get_dept(code)
        url = f"{BASE_URL}/{dept}/{code}/cadastre-{code}-parcelles.json.gz"
        dest = output_dir / f"parcelles-{code}.json.gz"

        print(f"[{i:3d}/{total}] {code}", end=" ")
        if download_file(url, dest, skip_existing=skip):
            downloaded += 1
            time.sleep(args.delay)
        elif skip and dest.exists():
            skipped += 1
        else:
            errors += 1

    print(f"\n{'=' * 50}")
    print(f"Terminé : {downloaded} téléchargé(s), {skipped} déjà présent(s), {errors} erreur(s)")
    print(f"Fichiers dans : {output_dir.resolve()}")


if __name__ == "__main__":
    run()
