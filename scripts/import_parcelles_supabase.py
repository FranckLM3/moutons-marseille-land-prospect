#!/usr/bin/env python3
"""
Import des parcelles pâturables dans Supabase PostGIS.

Usage :
    python scripts/import_parcelles_supabase.py
    python scripts/import_parcelles_supabase.py --geojson docs/pasture_zones.geojson
    python scripts/import_parcelles_supabase.py --dry-run   # vérifie sans insérer
    python scripts/import_parcelles_supabase.py --commune "Marseille 1er Arrondissement"

Variables d'environnement requises (.env) :
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_SERVICE_KEY=eyJ...   # clé service (pas la clé anon !)

Dépendances :
    pip install requests python-dotenv tqdm
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

SUPABASE_URL     = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
TABLE            = "parcelles"
BATCH_SIZE       = 500   # nombre de features par requête POST
MAX_RETRIES      = 3
RETRY_DELAY      = 2     # secondes


def check_env():
    if not SUPABASE_URL or SUPABASE_URL.startswith("__"):
        print("❌ SUPABASE_URL manquant dans .env")
        sys.exit(1)
    if not SUPABASE_SERVICE_KEY or SUPABASE_SERVICE_KEY.startswith("__"):
        print("❌ SUPABASE_SERVICE_KEY manquant dans .env")
        print("   → Récupère-la dans Supabase : Settings → API → service_role key")
        sys.exit(1)


def postgrest_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",  # upsert sur la PK id
    }


def _simplify_geom(geom: dict, decimal_places: int = 5) -> dict:
    """Arrondit les coordonnées GeoJSON à `decimal_places` décimales.

    Réduit la taille du payload envoyé à Supabase (~40% de gain sur les polygones).
    5 décimales ≈ précision 1 m — suffisant pour usage pastoral.
    """
    def round_coord(c):
        if isinstance(c[0], list):
            return [round_coord(x) for x in c]
        return [round(c[0], decimal_places), round(c[1], decimal_places)]

    result = dict(geom)
    if "coordinates" in result:
        result["coordinates"] = round_coord(result["coordinates"])
    return result


def feature_to_row(feat: dict) -> dict | None:
    """Convertit un feature GeoJSON en ligne Supabase.

    - Supprime area_ha (redondant avec area_m2 / 10000)
    - Supprime prairie_m2 (redondant, calculé dans le RPC)
    - Arrondit les coordonnées à 5 décimales (≈1 m) pour réduire le stockage
    """
    props = feat.get("properties") or {}
    geom  = feat.get("geometry")
    if not geom:
        return None
    fid = props.get("id")
    if not fid:
        return None
    return {
        "id":           str(fid),
        "area_m2":      props.get("area_m2"),
        "denomination": props.get("denomination"),
        "siren":        str(props["siren"]) if props.get("siren") else None,
        "nom_commune":       props.get("nom_commune") or "",
        "pct_prairie":       props.get("pct_prairie"),
        "cs_detail":         props.get("cs_detail"),
        "proprietaire_type": props.get("proprietaire_type"),
        # Géométrie arrondie à 5 décimales (≈1 m) avant envoi
        "geom_geojson":      json.dumps(_simplify_geom(geom, decimal_places=5)),
    }


def upsert_batch(rows: list[dict], dry_run: bool) -> int:
    """Envoie un batch via la RPC upsert_parcelles_batch (gère ST_GeomFromGeoJSON côté SQL)."""
    if dry_run:
        return len(rows)
    url = f"{SUPABASE_URL}/rest/v1/rpc/upsert_parcelles_batch"
    headers = {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {"rows": rows}
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        if resp.status_code in (200, 201):
            return len(rows)
        print(f"  ⚠️  HTTP {resp.status_code} (tentative {attempt}/{MAX_RETRIES}) : {resp.text[:300]}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY * attempt)
    raise RuntimeError(f"Échec après {MAX_RETRIES} tentatives")


def _refresh_communes_view():
    """Rafraîchit la materialized view communes_list après un import."""
    url = f"{SUPABASE_URL}/rest/v1/rpc/refresh_communes_list"
    headers = {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }
    resp = requests.post(url, headers=headers, json={}, timeout=60)
    if resp.status_code in (200, 204):
        print("   → communes_list rafraîchie ✓")
    else:
        print(f"   ⚠ REFRESH communes_list : HTTP {resp.status_code} {resp.text[:100]}")


def run(geojson_path: Path, dry_run: bool, filter_commune: str | None):
    check_env()

    print(f"📂 Lecture {geojson_path}…")
    with open(geojson_path, encoding="utf-8") as f:
        data = json.load(f)
    features = data.get("features") or []
    print(f"   {len(features):,} features chargées")

    # Exclure les parcelles sans aucune info prairie (prairie_m2 absent/null = pas croisées avec OCS GE)
    before = len(features)
    features = [
        feat for feat in features
        if (feat.get("properties") or {}).get("prairie_m2") is not None
    ]
    print(f"   → {len(features):,} features avec prairie analysée (−{before - len(features):,} sans données OCS GE)")

    # Filtre surface pâturable minimale : ne garder que prairie_m2 >= 1000 m²
    # Réduit drastiquement le volume en écartant parcelles urbaines/minérales sans intérêt pastoral
    MIN_PRAIRIE_M2 = 1000
    before = len(features)
    features = [
        feat for feat in features
        if (feat.get("properties") or {}).get("prairie_m2", 0) >= MIN_PRAIRIE_M2
    ]
    print(f"   → {len(features):,} features avec prairie ≥ {MIN_PRAIRIE_M2} m² (−{before - len(features):,} exclues)")

    # Filtre optionnel sur commune
    if filter_commune:
        features = [
            feat for feat in features
            if filter_commune.lower() in (feat.get("properties", {}).get("nom_commune") or "").lower()
        ]
        print(f"   → {len(features):,} features après filtre commune '{filter_commune}'")

    # Convertir en lignes
    rows = []
    skipped = 0
    for feat in features:
        row = feature_to_row(feat)
        if row is None:
            skipped += 1
        else:
            rows.append(row)
    if skipped:
        print(f"   ⚠️  {skipped} features ignorées (pas d'id ou pas de géométrie)")

    if not rows:
        print("❌ Aucune ligne à importer.")
        return

    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Import de {len(rows):,} parcelles "
          f"dans {SUPABASE_URL}/rest/v1/{TABLE}…")
    print(f"   Batch size : {BATCH_SIZE} → {len(rows) // BATCH_SIZE + 1} batches\n")

    total_ok = 0
    batches = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]

    if HAS_TQDM:
        iter_batches = tqdm(batches, unit="batch")
    else:
        iter_batches = batches

    t0 = time.time()
    for i, batch in enumerate(iter_batches, 1):
        try:
            ok = upsert_batch(batch, dry_run)
            total_ok += ok
        except RuntimeError as e:
            print(f"\n❌ Erreur batch {i}: {e}")
            sys.exit(1)
        if not HAS_TQDM and i % 10 == 0:
            pct = i / len(batches) * 100
            elapsed = time.time() - t0
            eta = elapsed / i * (len(batches) - i)
            print(f"   [{i}/{len(batches)}] {pct:.0f}% — {total_ok:,} lignes — ETA {eta:.0f}s")

    elapsed = time.time() - t0
    verb = "simulées" if dry_run else "importées"
    print(f"\n✅ {total_ok:,} lignes {verb} en {elapsed:.1f}s")
    if not dry_run:
        print(f"   → Vérifie dans Supabase : Table Editor → {TABLE}")
        _refresh_communes_view()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--geojson",  default=str(ROOT / "docs" / "pasture_zones.geojson"),
                        help="Chemin vers le GeoJSON à importer")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Simule l'import sans envoyer de données")
    parser.add_argument("--commune",  default=None,
                        help="Importer uniquement les parcelles d'une commune (test)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        geojson_path=Path(args.geojson),
        dry_run=args.dry_run,
        filter_commune=args.commune,
    )
