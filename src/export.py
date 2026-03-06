"""
Optimisation et export du GeoJSON final pour la cartographie web.
"""

from __future__ import annotations

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
KEEP_COLS_OWNERS = ["denomination", "siren", "nom_commune", "pct_prairie", "prairie_m2", "cs_detail"]


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
