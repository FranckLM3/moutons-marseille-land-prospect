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
