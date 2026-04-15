#!/usr/bin/env python3
"""
Télécharge les polygones communes depuis geo.api.gouv.fr et les stocke en statique.
Sortie : docs/data/geo/communes-paca.geojson

Départements inclus :
  PACA        : 04 05 06 13 83 84
  Frontières  : 01 07 26 30 34 38 48 73  (transhumance hors PACA)

Usage :
  python scripts/download_communes_geo.py
  python scripts/download_communes_geo.py --depts 13 83 84
"""
import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

from shapely.geometry import shape, mapping

PACA_DEPTS      = ["04", "05", "06", "13", "83", "84"]
BORDER_DEPTS    = ["01", "07", "26", "30", "34", "38", "48", "73"]
DEFAULT_DEPTS   = PACA_DEPTS + BORDER_DEPTS

BASE_URL = "https://geo.api.gouv.fr/departements/{dept}/communes?fields=nom,code&format=geojson&geometry=contour"

OUT_PATH = Path(__file__).parent.parent / "docs" / "data" / "geo" / "communes-paca.geojson"


def download_dept(dept: str) -> list[dict]:
    url = BASE_URL.format(dept=dept)
    req = urllib.request.Request(url, headers={"User-Agent": "MoutonsMarseillais/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
            features = data.get("features", [])
            print(f"  dept {dept}: {len(features)} communes")
            return features
    except Exception as e:
        print(f"  dept {dept}: ERREUR — {e}", file=sys.stderr)
        return []


def build_bbox(geometry: dict) -> list[float] | None:
    """Calcule [minLng, minLat, maxLng, maxLat] depuis une géométrie GeoJSON."""
    try:
        coords_flat: list[list[float]] = []
        def flatten(c):
            if not c:
                return
            if isinstance(c[0], (int, float)):
                coords_flat.append(c)
            else:
                for sub in c:
                    flatten(sub)
        geom_type = geometry.get("type", "")
        if geom_type == "Polygon":
            flatten(geometry["coordinates"][0])
        elif geom_type == "MultiPolygon":
            for poly in geometry["coordinates"]:
                flatten(poly[0])
        else:
            return None
        if not coords_flat:
            return None
        lngs = [c[0] for c in coords_flat]
        lats = [c[1] for c in coords_flat]
        return [min(lngs), min(lats), max(lngs), max(lats)]
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--depts", nargs="+", default=DEFAULT_DEPTS,
                        help="Codes département à télécharger (ex: 13 83 84)")
    args = parser.parse_args()

    depts = [d.zfill(2) for d in args.depts]
    print(f"Téléchargement de {len(depts)} département(s) : {' '.join(depts)}")

    all_features: list[dict] = []
    for i, dept in enumerate(depts):
        features = download_dept(dept)
        # Simplification + bbox pour chaque feature
        for f in features:
            geom = f.get("geometry")
            if geom:
                try:
                    s = shape(geom).simplify(0.001, preserve_topology=True)
                    f["geometry"] = mapping(s)
                    geom = f["geometry"]
                except Exception:
                    pass
            bbox = build_bbox(geom or {})
            if bbox:
                f["properties"]["bbox"] = bbox
        all_features.extend(features)
        if i < len(depts) - 1:
            time.sleep(0.15)  # politesse API

    # Supprimer la géométrie "contour" imbriquée dans properties si présente
    # (geo.api.gouv.fr la met dans feature.geometry, pas properties)
    for f in all_features:
        f["properties"].pop("contour", None)

    geojson = {
        "type": "FeatureCollection",
        "features": all_features,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(geojson, ensure_ascii=False, separators=(",", ":")))

    size_kb = OUT_PATH.stat().st_size // 1024
    print(f"\nSauvegardé : {OUT_PATH}")
    print(f"Total : {len(all_features)} communes — {size_kb} KB")


if __name__ == "__main__":
    main()
