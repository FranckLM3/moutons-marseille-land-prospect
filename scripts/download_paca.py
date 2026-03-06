#!/usr/bin/env python3
"""
Télécharge et prépare toutes les données nécessaires pour couvrir la région PACA :
  - Cadastre Etalab (parcelles) pour chaque département
  - OCS GE IGN (occupation du sol) pour chaque département

Départements PACA :
  04 — Alpes-de-Haute-Provence
  05 — Hautes-Alpes
  06 — Alpes-Maritimes
  13 — Bouches-du-Rhône   (déjà traité, skip par défaut)
  83 — Var
  84 — Vaucluse

Usage :
    python scripts/download_paca.py               # tous les départements sauf 13
    python scripts/download_paca.py --dept 83 84  # seulement Var + Vaucluse
    python scripts/download_paca.py --all         # inclut le 13

Sources :
  Cadastre : https://cadastre.data.gouv.fr/datasets/cadastre-etalab
  OCS GE   : https://geoservices.ign.fr/ocsge (téléchargement manuel requis, voir ci-dessous)

⚠️  L'OCS GE n'est PAS téléchargeable automatiquement (authentification IGN requise).
    Ce script télécharge uniquement le cadastre.
    Pour l'OCS GE, télécharge manuellement depuis :
    https://geoservices.ign.fr/ocsge
    Et place le .gpkg dans data/ocsge/D<dept>/OCCUPATION_SOL.gpkg
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ── Données des départements PACA ──────────────────────────────────────────
PACA_DEPTS = {
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "13": "Bouches-du-Rhône",
    "83": "Var",
    "84": "Vaucluse",
}

# Codes INSEE par département (source : geo.api.gouv.fr)
# Récupérés via l'API : https://geo.api.gouv.fr/departements/<dept>/communes?fields=code&format=json
DEPT_COMMUNE_CODES: dict[str, list[str]] = {}  # rempli dynamiquement via l'API

BASE_CADASTRE_URL = "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/communes"
GEO_API_URL = "https://geo.api.gouv.fr/departements/{dept}/communes?fields=code,nom&format=json"

# OCS GE : millésimes disponibles par département (à mettre à jour selon les téléchargements IGN)
OCSGE_GPKG_PATHS: dict[str, Path] = {
    "04": ROOT / "data/ocsge/D04/OCCUPATION_SOL.gpkg",
    "05": ROOT / "data/ocsge/D05/OCCUPATION_SOL.gpkg",
    "06": ROOT / "data/ocsge/D06/OCCUPATION_SOL.gpkg",
    "13": ROOT / "data/ocsge/OCS-GE_2-0__GPKG_LAMB93_D013_2023-01-01/OCS-GE/1_DONNEES_LIVRAISON_2025-05-00027/OCSGE_2-0_GPKG_LAMB93_D13-2023/OCCUPATION_SOL.gpkg",
    "83": ROOT / "data/ocsge/D83/OCCUPATION_SOL.gpkg",
    "84": ROOT / "data/ocsge/D84/OCCUPATION_SOL.gpkg",
}


def fetch_commune_codes(dept: str) -> list[str]:
    """Récupère les codes INSEE des communes d'un département via l'API geo.gouv.fr."""
    url = GEO_API_URL.format(dept=dept)
    print(f"  📡 Récupération des communes du département {dept}…")
    with urllib.request.urlopen(url, timeout=30) as resp:
        communes = json.loads(resp.read())
    codes = [c["code"] for c in communes]
    print(f"     → {len(codes)} communes trouvées")
    return codes


def download_cadastre_dept(dept: str, output_dir: Path, skip_existing: bool = True) -> int:
    """Télécharge le cadastre de toutes les communes d'un département."""
    output_dir.mkdir(parents=True, exist_ok=True)

    codes = fetch_commune_codes(dept)

    # Marseille (13055) → arrondissements 13201–13216
    if dept == "13":
        codes = [c for c in codes if c != "13055"]
        codes += [f"132{str(i).zfill(2)}" for i in range(1, 17)]

    downloaded = 0
    skipped = 0
    errors = 0

    for code in codes:
        out_path = output_dir / f"cadastre-{code}-parcelles.json"
        if skip_existing and out_path.exists():
            skipped += 1
            continue

        url = f"{BASE_CADASTRE_URL}/{code}/cadastre-{code}-parcelles.json.gz"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                gz_data = resp.read()
            with gzip.open(__import__('io').BytesIO(gz_data)) as f:
                out_path.write_bytes(f.read())
            downloaded += 1
            if downloaded % 20 == 0:
                print(f"     {downloaded}/{len(codes) - skipped} téléchargés…")
            time.sleep(0.1)  # politesse
        except Exception as e:
            errors += 1
            print(f"  ⚠️  {code} : {e}")

    print(f"  ✅ Dept {dept} : {downloaded} téléchargés, {skipped} déjà présents, {errors} erreurs")
    return downloaded


def check_ocsge(dept: str) -> bool:
    """Vérifie si le fichier OCS GE est disponible pour ce département."""
    path = OCSGE_GPKG_PATHS.get(dept)
    if path and path.exists():
        print(f"  ✅ OCS GE dept {dept} : {path.name} trouvé")
        return True
    else:
        print(f"  ⚠️  OCS GE dept {dept} : fichier manquant")
        print(f"     Télécharge depuis https://geoservices.ign.fr/ocsge")
        print(f"     Et place-le dans : {OCSGE_GPKG_PATHS.get(dept, ROOT / f'data/ocsge/D{dept}/OCCUPATION_SOL.gpkg')}")
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dept", nargs="+", choices=list(PACA_DEPTS.keys()),
                        help="Départements à traiter (défaut : tous sauf 13)")
    parser.add_argument("--all", action="store_true",
                        help="Inclure le département 13 (déjà traité)")
    parser.add_argument("--cadastre-only", action="store_true",
                        help="Télécharger uniquement le cadastre (pas de vérif OCS GE)")
    parser.add_argument("--output-dir", default=str(ROOT / "data/cadastre"),
                        help="Dossier de sortie du cadastre")
    parser.add_argument("--no-skip", action="store_true",
                        help="Re-télécharger même si le fichier existe déjà")
    return parser.parse_args()


def main():
    args = parse_args()

    depts = args.dept or ([k for k in PACA_DEPTS] if args.all else [k for k in PACA_DEPTS if k != "13"])

    print(f"\n🌿 Téléchargement données PACA — départements : {', '.join(depts)}")
    print("=" * 60)

    output_dir = Path(args.output_dir)

    for dept in depts:
        print(f"\n── Département {dept} — {PACA_DEPTS[dept]} ──")

        # Cadastre
        print(f"  📦 Cadastre → {output_dir}/")
        download_cadastre_dept(dept, output_dir, skip_existing=not args.no_skip)

        # OCS GE (vérification seulement)
        if not args.cadastre_only:
            check_ocsge(dept)

    print("\n" + "=" * 60)
    print("✅ Téléchargement terminé.")
    print()
    print("📋 Prochaines étapes pour chaque département avec OCS GE disponible :")
    for dept in depts:
        gpkg = OCSGE_GPKG_PATHS.get(dept)
        if gpkg and gpkg.exists():
            print(f"  python scripts/build.py --gpkg {gpkg} --output data/pasture_{dept}.geojson")
    print()
    print("  Puis import dans Supabase :")
    for dept in depts:
        print(f"  python scripts/import_parcelles_supabase.py --geojson data/pasture_{dept}.geojson")


if __name__ == "__main__":
    main()
