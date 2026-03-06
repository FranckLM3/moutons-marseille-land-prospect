"""
Chargement des données de propriété (personnes morales) et jointure spatiale
avec les zones pâturables.

Source : Koumoul OpenData — Parcelles des personnes morales (département 13)
  https://opendata.koumoul.com/datasets/parcelles-des-personnes-morales
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

# Préfixes CS considérés comme "prairie" (haute valeur pâturale)
PRAIRIE_CS_PREFIXES = ("CS1.1.1.1", "CS1.1.1.2", "CS1.1.1.3")

# Mapping groupe_personne (champ MAJIC) → type de propriétaire
# Source : documentation MAJIC / nomenclature DGFiP
#   0  = Indivision / groupement (statut indéterminé)
#   1  = État
#   2  = Région
#   3  = Département
#   4  = Commune / EPCI / section de commune
#   5  = Organisme HLM, SEM de construction (bailleur social)
#   6  = Autre personne morale à statut public ou para-public (CDC, RFF…)
#   7  = Copropriété
#   8  = SCI / société civile
#   9  = Société privée (SA, SARL, SAS…)
GROUPE_PERSONNE_TYPE: dict[int, str] = {
    0: "indéterminé",
    1: "public",       # État
    2: "public",       # Région
    3: "public",       # Département
    4: "public",       # Commune / EPCI
    5: "semi-public",  # HLM / SEM construction
    6: "semi-public",  # Organisme para-public (CDC, SNCF…)
    7: "privé",        # Copropriété
    8: "privé",        # SCI
    9: "privé",        # Société privée
}


def load_owners(geojson_path: str | Path) -> gpd.GeoDataFrame:
    """Charge le fichier GeoJSON des parcelles des personnes morales.

    Parameters
    ----------
    geojson_path:
        Chemin vers ``parcelles-des-personnes-morales.geojson``.

    Returns
    -------
    GeoDataFrame en WGS84 (EPSG:4326) avec colonnes normalisées
    ``denomination`` et ``siren``.
    """
    path = Path(geojson_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier propriétaires introuvable : {path}\n"
            "Téléchargez-le depuis :\n"
            "  https://opendata.koumoul.com/datasets/parcelles-des-personnes-morales"
        )

    print(f"  → chargement propriétaires : {path.name}")
    owners = gpd.read_file(str(path))
    owners = owners.to_crs("EPSG:4326")

    # Normalisation des noms de colonnes
    owners = _normalize_columns(owners)

    keep = [c for c in ["denomination", "siren", "groupe_personne", "proprietaire_type", "nom_commune", "geometry"] if c in owners.columns]
    owners = owners[keep].copy()

    print(f"  → {len(owners):,} parcelles de personnes morales chargées")
    return owners


def spatial_join_owners(
    pasture: gpd.GeoDataFrame,
    owners: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Jointure spatiale entre les zones pâturables et les propriétaires.

    Seules les zones intersectant une parcelle de personne morale sont conservées.

    Parameters
    ----------
    pasture:
        GeoDataFrame des zones pâturables (WGS84).
    owners:
        GeoDataFrame des propriétaires (WGS84).

    Returns
    -------
    GeoDataFrame résultant de la jointure, sans doublons de colonnes.
    """
    # S'assurer que les deux sont en WGS84
    if pasture.crs.to_epsg() != 4326:
        pasture = pasture.to_crs("EPSG:4326")
    if owners.crs.to_epsg() != 4326:
        owners = owners.to_crs("EPSG:4326")

    print(f"  → jointure spatiale ({len(pasture):,} zones × {len(owners):,} parcelles)…")
    result = gpd.sjoin(pasture, owners, how="inner", predicate="intersects")

    # Nettoyage post-jointure
    if "index_right" in result.columns:
        result = result.drop(columns=["index_right"])
    result = result.loc[:, ~result.columns.duplicated()]

    # ── Calcul pct_prairie par parcelle propriétaire ──────────────────────
    # Pour chaque parcelle (identifiée par siren + denomination), on calcule :
    #   surface prairie (CS1.1.1.x) / surface totale de toutes ses zones OCS GE
    result = _add_pct_prairie(result)

    print(f"  → {len(result):,} zones avec propriétaire identifié")
    return result


def _add_pct_prairie(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ajoute la colonne ``pct_prairie`` (0–100) par parcelle propriétaire.

    Le ratio est calculé comme :
        surface des zones CS1.1.1.x / surface totale des zones OCS GE
    pour toutes les zones OCS GE appartenant à la même parcelle propriétaire
    (regroupées par siren quand disponible, sinon par denomination).

    La valeur est la même pour toutes les lignes d'une même parcelle.
    """
    gdf = gdf.copy()

    if "code_cs" not in gdf.columns or "area_m2" not in gdf.columns:
        gdf["pct_prairie"] = None
        return gdf

    # Clé de regroupement : siren si disponible, sinon denomination
    if "siren" in gdf.columns:
        group_key = gdf["siren"].fillna("").astype(str)
    elif "denomination" in gdf.columns:
        group_key = gdf["denomination"].fillna("").astype(str)
    else:
        gdf["pct_prairie"] = None
        return gdf

    # Booléen : est-ce une zone prairie ?
    is_prairie = gdf["code_cs"].astype(str).apply(
        lambda c: any(c.startswith(p) for p in PRAIRIE_CS_PREFIXES)
    )

    tmp = pd.DataFrame({
        "key":      group_key,
        "area_m2":  gdf["area_m2"].astype(float),
        "prairie":  is_prairie,
    })

    # Surface prairie et surface totale par parcelle propriétaire
    totals = tmp.groupby("key")["area_m2"].sum().rename("total_m2")
    prairie_totals = (
        tmp[tmp["prairie"]]
        .groupby("key")["area_m2"].sum()
        .rename("prairie_m2")
    )
    ratios = (prairie_totals / totals * 100).round(1).rename("pct_prairie")

    gdf["pct_prairie"] = group_key.map(ratios).fillna(0.0)
    return gdf


def _normalize_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Renomme les colonnes sources vers des noms canoniques et calcule proprietaire_type."""
    rename_map: dict[str, str] = {}

    col_lower = {c.lower(): c for c in gdf.columns}

    for src, dst in [
        ("denomination", "denomination"),
        ("denom", "denomination"),
        ("numero_siren", "siren"),
        ("siren", "siren"),
        ("_infos_commune.nom_commune", "nom_commune"),
        ("nom_commune", "nom_commune"),
        ("groupe_personne", "groupe_personne"),
    ]:
        if src in col_lower and dst not in rename_map.values():
            rename_map[col_lower[src]] = dst

    gdf = gdf.rename(columns=rename_map)

    # Calcul de proprietaire_type depuis groupe_personne
    if "groupe_personne" in gdf.columns:
        gdf["proprietaire_type"] = (
            gdf["groupe_personne"]
            .apply(lambda v: GROUPE_PERSONNE_TYPE.get(int(v), "indéterminé") if pd.notna(v) else "indéterminé")
        )

    return gdf
