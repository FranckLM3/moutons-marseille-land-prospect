"""
Filtrage des zones pâturables par critères de surface et localisation.
"""

from __future__ import annotations

import geopandas as gpd

# Surface minimale par défaut (m²)
DEFAULT_MIN_AREA_M2 = 5_000


def filter_by_area(
    gdf: gpd.GeoDataFrame,
    min_area_m2: float = DEFAULT_MIN_AREA_M2,
    max_area_m2: float | None = None,
    area_col: str = "area_m2",
) -> gpd.GeoDataFrame:
    """Conserve uniquement les polygones dont la surface est dans la plage donnée.

    La colonne ``area_col`` doit déjà exister (calculée en Lambert 93).
    Si elle est absente, elle est calculée automatiquement (la géométrie doit
    être en projection métrique).

    Parameters
    ----------
    gdf:
        GeoDataFrame à filtrer.
    min_area_m2:
        Surface minimale en m² (défaut : 5 000 m²).
    max_area_m2:
        Surface maximale en m² (optionnel).
    area_col:
        Nom de la colonne de surface.

    Returns
    -------
    GeoDataFrame filtré.
    """
    if area_col not in gdf.columns:
        if gdf.crs and gdf.crs.is_projected:
            gdf = gdf.copy()
            gdf[area_col] = gdf.geometry.area
        else:
            gdf_proj = gdf.to_crs("EPSG:2154")
            gdf = gdf.copy()
            gdf[area_col] = gdf_proj.geometry.area

    mask = gdf[area_col] >= min_area_m2
    if max_area_m2 is not None:
        mask &= gdf[area_col] <= max_area_m2

    result = gdf.loc[mask].copy()
    print(
        f"  → {len(result):,} polygones ≥ {min_area_m2:,.0f} m² "
        f"(sur {len(gdf):,} initiaux)"
    )
    return result


def add_area_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ajoute les colonnes area_m2 et area_ha (calcul en Lambert 93).

    Parameters
    ----------
    gdf:
        GeoDataFrame en n'importe quelle projection.

    Returns
    -------
    GeoDataFrame avec colonnes area_m2 et area_ha arrondies.
    """
    gdf = gdf.copy()
    gdf_proj = gdf.to_crs("EPSG:2154") if gdf.crs.to_epsg() != 2154 else gdf
    gdf["area_m2"] = gdf_proj.geometry.area.round(0)
    gdf["area_ha"] = (gdf_proj.geometry.area / 10_000).round(2)
    return gdf
