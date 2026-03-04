#!/usr/bin/env python3
"""
Moutons Marseille — Land Prospect
==================================
Pipeline de traitement géospatial pour identifier les terrains pâturables
dans la métropole Aix-Marseille-Provence.

Sources de données :
  OCS GE IGN 2023 D13 (occupation du sol, vecteur 1-2 m)
  Cadastre Etalab — parcelles Marseille (16 arrondissements)
  Parcelles des personnes morales — Koumoul OpenData

Unité de résultat : PARCELLE CADASTRALE
  - Géométrie = contour de la parcelle cadastrale
  - pct_prairie = surface prairie OCS GE / surface parcelle × 100

Usage :
  python scripts/build.py
  python scripts/build.py --min-area 10000
  python scripts/build.py --gpkg data/ocsge/my_file.gpkg --output docs/pasture_zones.geojson
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

# Ajouter la racine au path pour les imports src.*
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Les imports géospatiaux sont chargés uniquement quand le pipeline complet est exécuté
# (pas en mode --inject-only, pour éviter de dépendre de geopandas en CI légère)

# ---------------------------------------------------------------------------
# Chemins par défaut
# ---------------------------------------------------------------------------
DEFAULT_GPKG = (
    ROOT
    / "data" / "ocsge"
    / "OCS-GE_2-0__GPKG_LAMB93_D013_2023-01-01"
    / "OCS-GE" / "1_DONNEES_LIVRAISON_2025-05-00027"
    / "OCSGE_2-0_GPKG_LAMB93_D13-2023"
    / "OCCUPATION_SOL.gpkg"
)
DEFAULT_OWNERS   = ROOT / "data" / "parcelles-des-personnes-morales.geojson"
DEFAULT_CADASTRE = ROOT / "data" / "cadastre"
DEFAULT_OUTPUT   = ROOT / "docs" / "pasture_zones.geojson"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Génère le GeoJSON des zones pâturables (parcelles cadastrales).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--gpkg",     default=str(DEFAULT_GPKG))
    parser.add_argument("--owners",   default=str(DEFAULT_OWNERS))
    parser.add_argument("--cadastre", default=str(DEFAULT_CADASTRE))
    parser.add_argument("--output",   default=str(DEFAULT_OUTPUT))
    parser.add_argument("--min-area", type=float, default=5_000,
                        help="Surface minimale des parcelles en m² (défaut : 5000)")
    parser.add_argument("--max-area", type=float, default=None)
    parser.add_argument("--min-prairie", type=float, default=0.0,
                        help="pct prairie minimum (0-100, defaut : 0 = pas de filtre)")
    parser.add_argument("--include-without-owner", action="store_true",
                        help="Conserve les parcelles sans propriétaire (jointure gauche)")
    parser.add_argument("--inject-only", action="store_true",
                        help="Génère uniquement docs/index.html depuis le template (sans pipeline géospatial)")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def run(args: argparse.Namespace) -> None:
    # Imports géospatiaux chargés ici uniquement (pas nécessaires en --inject-only)
    from src.cadastre import add_prairie_ratio, join_owners_to_cadastre, load_cadastre
    from src.export import export_geojson, prepare_for_export
    from src.filters import add_area_columns, filter_by_area
    from src.land_cover import filter_pasture_zones, load_ocsge
    from src.owners import load_owners

    t0 = time.time()

    print("\n🐑 Moutons Marseille — Land Prospect")
    print("=" * 50)

    # ── Étape 1 : OCS GE ─────────────────────────────────────────────────
    print("\n[1/5] Chargement OCS GE…")
    ocsge_raw = load_ocsge(args.gpkg)

    # ── Étape 2 : filtrage zones pâturables OCS GE ────────────────────────
    print("\n[2/5] Filtrage zones pâturables OCS GE…")
    ocsge = filter_pasture_zones(ocsge_raw)

    if len(ocsge) == 0:
        print("❌ Aucune zone pâturable trouvée dans l'OCS GE.")
        sys.exit(1)

    # ── Étape 3 : parcelles cadastrales + propriétaires ───────────────────
    print("\n[3/5] Chargement cadastre + propriétaires…")
    cadastre = load_cadastre(args.cadastre)
    cadastre = add_area_columns(cadastre)
    cadastre = filter_by_area(cadastre, min_area_m2=args.min_area, max_area_m2=args.max_area)

    owners = load_owners(args.owners)
    cadastre = join_owners_to_cadastre(
        cadastre,
        owners,
        include_without_owner=args.include_without_owner,
    )

    if len(cadastre) == 0:
        print("❌ Aucune parcelle avec propriétaire identifié.")
        sys.exit(1)

    # ── Étape 4 : calcul % prairie par parcelle ───────────────────────────
    print("\n[4/5] Calcul % prairie par parcelle (intersection OCS GE)…")
    cadastre = add_prairie_ratio(cadastre, ocsge)

    # Filtre optionnel sur % prairie minimum
    if args.min_prairie > 0:
        before = len(cadastre)
        cadastre = cadastre[cadastre["pct_prairie"] >= args.min_prairie].copy()
        print(f"  → filtre ≥{args.min_prairie}% prairie : {before:,} → {len(cadastre):,} parcelles")

    if len(cadastre) == 0:
        print("❌ Aucune parcelle satisfait le critère % prairie.")
        sys.exit(1)

    # ── Étape 5 : export ──────────────────────────────────────────────────
    print("\n[5/5] Export GeoJSON…")
    cadastre = cadastre.to_crs("EPSG:4326")
    gdf = prepare_for_export(cadastre)
    export_geojson(gdf, args.output)

    elapsed = time.time() - t0
    print(f"\n✅ Terminé en {elapsed:.1f}s → {args.output}")

    # ── Injection clé ORS dans index.html ─────────────────────────────────
    _inject_ors_key(ROOT / "docs" / "index.html")

    print("\n💡 Prochaine étape :")
    print("   python -m http.server 8000 --directory docs --bind 127.0.0.1")
    print("   Ouvrir http://127.0.0.1:8000\n")


def _inject_ors_key(html_path: Path) -> None:
    """Génère docs/index.html depuis docs/index.template.html en injectant ORS_API_KEY.

    Le template référence app.css et app.js (fichiers versionnés dans docs/).
    Seul index.html est ignoré par git (il contient la clé en clair).
    """
    template_path = html_path.parent / "index.template.html"
    env_path = ROOT / ".env"
    ors_key = os.environ.get("ORS_API_KEY", "")
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_ANON_KEY", "")

    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if not ors_key and line.startswith("ORS_API_KEY="):
                ors_key = line.split("=", 1)[1].strip()
            if not supabase_url and line.startswith("SUPABASE_URL="):
                supabase_url = line.split("=", 1)[1].strip()
            if not supabase_key and line.startswith("SUPABASE_ANON_KEY="):
                supabase_key = line.split("=", 1)[1].strip()

    if not ors_key:
        print("⚠️  ORS_API_KEY non trouvée (.env ou variable d'environnement) — index.html non généré")
        return

    if not template_path.exists():
        print(f"⚠️  {template_path} introuvable — injection ignorée")
        return

    content = template_path.read_text(encoding="utf-8")
    content = content.replace("__ORS_KEY__", ors_key)
    content = content.replace("__SUPABASE_URL__", supabase_url)
    content = content.replace("__SUPABASE_ANON_KEY__", supabase_key)
    html_path.write_text(content, encoding="utf-8")
    print(f"  → {html_path.name} généré depuis template avec clé ORS injectée")


if __name__ == "__main__":
    args = parse_args()
    if args.inject_only:
        _inject_ors_key(ROOT / "docs" / "index.html")
    else:
        run(args)
