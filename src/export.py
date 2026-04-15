"""
Optimisation et export du GeoJSON final pour la cartographie web.
"""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import geopandas as gpd

# Colonnes cadastrales à conserver dans le GeoJSON de sortie
KEEP_COLS_CADASTRE = [
    "id",            # identifiant parcelle cadastrale
    "area_m2",
    "area_ha",
    "geometry",
]

# Colonnes propriétaires + OCS GE enrichies
KEEP_COLS_OWNERS = ["denomination", "siren", "nom_commune", "pct_prairie", "prairie_m2", "cs_detail", "proprietaire_type"]


def prepare_for_export(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Nettoie, simplifie les géométries et sélectionne les colonnes utiles.

    Parameters
    ----------
    gdf:
        GeoDataFrame résultat de la jointure (en WGS84 ou Lambert 93).

    Returns
    -------
    GeoDataFrame propre en WGS84, prêt pour export GeoJSON.
    """
    gdf = gdf.copy()

    # Projection Lambert 93 pour la simplification métrique
    gdf_proj = gdf.to_crs("EPSG:2154")
    print("  → simplification des géométries (tolérance 2 m)…")
    gdf_proj["geometry"] = gdf_proj["geometry"].simplify(
        tolerance=2, preserve_topology=True
    )
    gdf = gdf_proj.to_crs("EPSG:4326")

    # Arrondi des surfaces
    if "area_m2" in gdf.columns:
        gdf["area_m2"] = gdf["area_m2"].round(0).astype(int)
    if "area_ha" in gdf.columns:
        gdf["area_ha"] = gdf["area_ha"].round(2)

    # Surface pâturable absolue (m²) = area_m2 × pct_prairie / 100
    if "area_m2" in gdf.columns and "pct_prairie" in gdf.columns:
        prairie_m2 = (gdf["area_m2"] * gdf["pct_prairie"] / 100).round(0)
        gdf["prairie_m2"] = prairie_m2.where(prairie_m2.notna(), None)

    # Sélection des colonnes disponibles
    desired = KEEP_COLS_CADASTRE + KEEP_COLS_OWNERS
    cols = [c for c in desired if c in gdf.columns]
    # Toujours inclure geometry
    if "geometry" not in cols:
        cols.append("geometry")
    gdf = gdf[cols].copy()

    # Supprimer les doublons de lignes éventuels
    gdf = gdf.drop_duplicates()

    print(f"  → {len(gdf):,} features, {len(gdf.columns)} colonnes")
    return gdf


def _slugify(text: str) -> str:
    """Convertit un nom de commune en slug ASCII safe pour un nom de fichier."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    for ch in (" ", "-", "'", "\u2019"):
        text = text.replace(ch, "_")
    text = "".join(c for c in text if c.isalnum() or c == "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def export_by_commune(gdf: gpd.GeoDataFrame, output_dir: str | Path) -> list[dict]:
    """Exporte un fichier GeoJSON par commune + un ``index.json`` avec bbox.

    Parameters
    ----------
    gdf:
        GeoDataFrame préparé (WGS84) avec colonne ``nom_commune``.
    output_dir:
        Répertoire de sortie (ex. ``docs/data/communes/``).

    Returns
    -------
    Liste de dicts index : ``[{"name": ..., "file": ..., "bbox": [minLng, minLat, maxLng, maxLat]}, ...]``
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    communes = sorted(gdf["nom_commune"].dropna().unique())
    print(f"  → {len(communes)} communes à exporter…")

    index: list[dict] = []
    for name in communes:
        sub = gdf[gdf["nom_commune"] == name].copy()
        slug = _slugify(name)
        filename = f"{slug}.geojson"
        path = output_dir / filename

        sub.to_file(str(path), driver="GeoJSON", mode="w")

        # total_bounds retourne (minx, miny, maxx, maxy) en WGS84 = (minLng, minLat, maxLng, maxLat)
        minx, miny, maxx, maxy = sub.total_bounds
        index.append({
            "name": name,
            "file": filename,
            "bbox": [round(float(minx), 6), round(float(miny), 6),
                     round(float(maxx), 6), round(float(maxy), 6)],
        })
        print(f"    {name} → {filename} ({len(sub):,} parcelles)")

    # Merger avec un index existant (cas multi-département)
    index_path = output_dir / "index.json"
    if index_path.exists():
        try:
            existing = json.loads(index_path.read_text(encoding="utf-8"))
            existing_by_name = {e["name"]: e for e in existing}
            for entry in index:
                existing_by_name[entry["name"]] = entry  # écrase si même nom
            index = sorted(existing_by_name.values(), key=lambda e: e["name"])
        except (json.JSONDecodeError, KeyError):
            pass  # index corrompu → on écrase

    index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  ✓ index.json → {len(index)} communes au total")

    return index


def export_geojson(gdf: gpd.GeoDataFrame, output_path: str | Path) -> Path:
    """Exporte le GeoDataFrame en GeoJSON.

    Parameters
    ----------
    gdf:
        GeoDataFrame en WGS84.
    output_path:
        Chemin de sortie (ex. ``docs/pasture_zones.geojson``).

    Returns
    -------
    Chemin absolu du fichier écrit.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"  → écriture {output_path} …")
    gdf.to_file(str(output_path), driver="GeoJSON", mode="w")
    size_mb = output_path.stat().st_size / 1_048_576
    print(f"  ✓ {len(gdf):,} features → {output_path.name} ({size_mb:.1f} MB)")
    return output_path.resolve()
