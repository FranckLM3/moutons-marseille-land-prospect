"""
Chargement et filtrage de l'OCS GE IGN v2.

Source : https://geoservices.ign.fr/ocsge
Format : GeoPackage (.gpkg), couche OCCUPATION_SOL
Département 13 – millésime 2020/2023

Nomenclature OCS GE v2 — codes CS retenus pour le pâturage :
  CS2.2.1    Formations herbacées (pelouses, prairies) — IDÉAL, base pct_prairie
             105 419 polygones bruts dans le dep. 13
  CS2.1.2    Formations arbustives (landes, maquis, garrigue) — typique du massif marseillais
             13 120 polygones
  CS2.1.1.1  Peuplement de feuillus — pâturage sous-bois possible
             38 058 polygones
  CS2.1.1.3  Peuplement mixte
             11 533 polygones
  CS2.2.2    Autres formations non ligneuses (mousses, lichens, zones humides)
             absent du dep. 13 (0 polygone) — conservé par cohérence de nomenclature

Codes CS volontairement EXCLUS du filtre :
  CS2.1.1.2  Peuplement de conifères (pins parasol, pins d'Alep, massif des Calanques)
             31 803 polygones bruts, dont 21 727 avec un usage pastoral (US1.2/US5/US235).
             CHOIX : exclu car le sous-bois de pins méditerranéens offre peu d'herbage
             consommable (sol rocheux, litière acide) et le risque incendie y est élevé.
             Si l'on souhaite explorer ces zones à l'avenir, ajouter "CS2.1.1.2"
             dans medium_priority ou low_priority.
  CS2.1.3    Autres formations ligneuses (vignes, oliviers, vergers)
             3 326 polygones, quasi-exclusivement en usage agricole US1.1.
             CHOIX : exclu car ce sont des cultures pérennes non grazable sans dommage.

Codes US compatibles retenus :
  US1.1 / US1.2 / US1.3 / US1.4   Agriculture (prairies, sylviculture…)
  US5                               Espaces ouverts / naturels / friches
  US235                             Forêts et espaces semi-naturels

Codes US incompatibles (exclus) :
  US2 / US3 / US4.x   Industrie, commerce, habitat, équipements
  US6.x                Réseaux (routes, rails, ports)
"""

from pathlib import Path

import geopandas as gpd

# ---------------------------------------------------------------------------
# Codes CS (Couverture du Sol) par niveau de priorité — Nomenclature OCS GE v2
# ---------------------------------------------------------------------------
PASTURE_CS_CODES: dict[str, list[str]] = {
    "high_priority": [
        "CS2.2.1",    # Formations herbacées (pelouses, prairies) — IDÉAL
    ],
    "medium_priority": [
        "CS2.1.2",    # Formations arbustives (landes, maquis — pâturage possible)
        "CS2.1.1.1",  # Peuplement de feuillus (pâturage sous-bois)
        "CS2.1.1.3",  # Peuplement mixte
        "CS2.2.2",    # Autres formations non ligneuses
    ],
    # Codes low_priority : listés pour mémoire mais NON utilisés dans PASTURE_CS_PREFIXES
    # (voir docstring du module pour le raisonnement détaillé)
    "low_priority": [
        "CS2.1.3",    # Autres formations ligneuses (vignes, oliviers) — cultures pérennes, exclu
        "CS2.1.1.2",  # Peuplement de conifères (pins Calanques) — sous-bois pauvre, risque feu, exclu
    ],
}

# Prefixes plats utilisés pour le filtrage spatial (high + medium uniquement)
PASTURE_CS_PREFIXES: tuple[str, ...] = tuple(
    PASTURE_CS_CODES["high_priority"] + PASTURE_CS_CODES["medium_priority"]
)

# ---------------------------------------------------------------------------
# Libellés officiels des codes CS (Couverture du Sol) — Nomenclature OCS GE v2
# ---------------------------------------------------------------------------
CS_CODE_LABELS: dict[str, str] = {
    # CS1. Sans végétation
    "CS1.1":     "Surfaces anthropisées",
    "CS1.1.1":   "Zones bâties",
    "CS1.1.1.1": "Zones bâties",
    "CS1.1.1.2": "Zones non bâties (routes, parkings)",
    "CS1.1.2":   "Zones à matériaux minéraux",
    "CS1.1.2.1": "Matériaux minéraux (chemins, carrières)",
    "CS1.1.2.2": "Matériaux composites (décharges)",
    "CS1.2":     "Surfaces naturelles",
    "CS1.2.1":   "Sols nus (sable, rochers)",
    "CS1.2.2":   "Surfaces d'eau",
    "CS1.2.3":   "Névés et glaciers",
    # CS2. Avec végétation
    "CS2.1":     "Végétation ligneuse",
    "CS2.1.1":   "Formations arborées",
    "CS2.1.1.1": "Peuplement de feuillus",
    "CS2.1.1.2": "Peuplement de conifères",
    "CS2.1.1.3": "Peuplement mixte",
    "CS2.1.2":   "Formations arbustives (landes, maquis)",
    "CS2.1.3":   "Autres formations ligneuses (vignes)",
    "CS2.2":     "Végétation non ligneuse",
    "CS2.2.1":   "Formations herbacées (prairies)",
    "CS2.2.2":   "Autres formations non ligneuses",
}

# ---------------------------------------------------------------------------
# Codes US (Usage du Sol) — Nomenclature OCS GE v2
# ---------------------------------------------------------------------------

# Codes US COMPATIBLES avec le pâturage
PASTURE_US_CODES: set[str] = {
    "US1.1",    # Agriculture — terres arables
    "US1.2",    # Agriculture — prairies permanentes (IDÉAL)
    "US1.3",    # Agriculture — vergers, vignes
    "US1.4",    # Agriculture — autres zones agricoles
    "US5",      # Espaces ouverts sans usage (friches, nature)
    "US235",    # Forêts et espaces semi-naturels
}

# Codes US INCOMPATIBLES — à exclure absolument
INCOMPATIBLE_US_CODES: set[str] = {
    "US2",      # Industrie / activités économiques
    "US3",      # Zones commerciales
    "US4.1.1",  # Habitat individuel
    "US4.1.2",  # Habitat collectif
    "US4.1.3",  # Habitat mixte
    "US4.1.4",  # Habitat informel
    "US4.2",    # Activités tertiaires
    "US4.3",    # Équipements publics
    "US6.1",    # Réseaux routiers
    "US6.2",    # Réseaux ferrés
    "US6.3",    # Zones portuaires et aéroportuaires
}

# Couche principale dans le GeoPackage OCS GE v2
LAYER_OCCUPATION = "OCCUPATION_SOL"


def load_ocsge(gpkg_path: str | Path) -> gpd.GeoDataFrame:
    """Charge la couche OCCUPATION_SOL depuis le GeoPackage OCS GE.

    Parameters
    ----------
    gpkg_path:
        Chemin vers le fichier .gpkg téléchargé (après extraction du .7z).

    Returns
    -------
    GeoDataFrame en Lambert 93 (EPSG:2154).
    """
    gpkg_path = Path(gpkg_path)
    if not gpkg_path.exists():
        raise FileNotFoundError(
            f"OCS GE GeoPackage introuvable : {gpkg_path}\n"
            "Téléchargez-le depuis :\n"
            "  https://data.geopf.fr/telechargement/download/OCSGE/"
            "OCS-GE_2-0__GPKG_LAMB93_D013_2023-01-01/"
            "OCS-GE_2-0__GPKG_LAMB93_D013_2023-01-01.7z\n"
            "Puis extrayez le .7z et placez le .gpkg dans data/ocsge/"
        )
    print(f"  → chargement OCS GE : {gpkg_path.name}")
    gdf = gpd.read_file(str(gpkg_path), layer=LAYER_OCCUPATION)
    # Assurer la projection Lambert 93
    if gdf.crs is None or gdf.crs.to_epsg() != 2154:
        gdf = gdf.set_crs("EPSG:2154", allow_override=True)
    return gdf


def filter_pasture_cs_only(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Filtre les zones pâturables selon la couverture (CS) uniquement, sans filtre US.

    Utilisé pour le calcul de pct_prairie : on veut mesurer la vraie surface
    végétale pâturable quelle que soit l'affectation administrative (usage).
    Le filtre US (usage) sert uniquement à sélectionner quelles parcelles
    apparaissent dans les résultats, pas à calculer leur couverture végétale.

    Parameters
    ----------
    gdf:
        GeoDataFrame issu de load_ocsge().

    Returns
    -------
    Sous-ensemble des polygones avec couverture herbacée/lande/arborée pâturable.
    """
    cs_col = _find_column(gdf, ["code_cs", "CS", "cs"])
    if cs_col is None:
        raise ValueError(
            f"Colonne 'code_cs' introuvable. Colonnes disponibles : {list(gdf.columns)}"
        )
    cs_values = gdf[cs_col].astype(str)
    mask = cs_values.apply(lambda v: any(v.startswith(p) for p in PASTURE_CS_PREFIXES))
    result = gdf.loc[mask].copy()
    print(
        f"  → {len(result):,} polygones CS pâturables (filtre CS seul, sans US) "
        f"(sur {len(gdf):,} polygones OCS GE)"
    )
    return result


def filter_pasture_zones(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Filtre les zones pâturables selon la couverture (CS) ET l'usage (US).

    Utilisé pour sélectionner les parcelles cadastrales à afficher : seules
    celles dont l'OCS GE indique un usage agricole/naturel sont retenues.

    Parameters
    ----------
    gdf:
        GeoDataFrame issu de load_ocsge().

    Returns
    -------
    Sous-ensemble des polygones avec couverture herbacée/lande ET usage
    agricole ou naturel.
    """
    cs_col = _find_column(gdf, ["code_cs", "CS", "cs"])
    us_col = _find_column(gdf, ["code_us", "US", "us"])

    if cs_col is None:
        raise ValueError(
            f"Colonne 'code_cs' introuvable. Colonnes disponibles : {list(gdf.columns)}"
        )

    # Filtre couverture herbacée / lande
    cs_values = gdf[cs_col].astype(str)
    mask_cs = cs_values.apply(
        lambda v: any(v.startswith(p) for p in PASTURE_CS_PREFIXES)
    )

    # Filtre usage si la colonne existe
    if us_col is not None:
        us_values = gdf[us_col].astype(str)
        mask_us = us_values.isin(PASTURE_US_CODES)
        mask = mask_cs & mask_us
    else:
        print("  ⚠ colonne usage (code_us) absente — filtrage sur couverture seule")
        mask = mask_cs

    result = gdf.loc[mask].copy()
    print(
        f"  → {len(result):,} zones pâturables conservées "
        f"(sur {len(gdf):,} polygones OCS GE)"
    )
    return result


def _find_column(gdf: gpd.GeoDataFrame, candidates: list[str]) -> str | None:
    """Retourne le premier nom de colonne trouvé parmi les candidats."""
    cols_lower = {c.lower(): c for c in gdf.columns}
    for name in candidates:
        if name.lower() in cols_lower:
            return cols_lower[name.lower()]
    return None
