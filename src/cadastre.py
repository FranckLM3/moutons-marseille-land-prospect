"""
Chargement des parcelles cadastrales (Etalab) et calcul du % prairie
par intersection spatiale avec l'OCS GE.

Source cadastre : https://cadastre.data.gouv.fr/datasets/cadastre-etalab
  Parcelles par commune AMP :
    https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/communes/<dept>/<code>/
    cadastre-<code>-parcelles.json.gz

  Marseille (13055) est découpée en 16 arrondissements : codes 13201 à 13216.
  Saint-Zacharie (83120) est dans le Var, Pertuis (84089) dans le Vaucluse.
"""

from __future__ import annotations

import gzip
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import pandas as pd

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

# Codes INSEE des 16 arrondissements de Marseille (cadastre Etalab)
MARSEILLE_CODES: list[str] = [f"132{i:02d}" for i in range(1, 17)]  # 13201 … 13216

# Noms des arrondissements de Marseille (13201 = 1er, …, 13216 = 16e)
_MARSEILLE_NAMES: dict[str, str] = {
    f"132{i:02d}": f"Marseille {i}{'er' if i == 1 else 'e'} Arrondissement"
    for i in range(1, 17)
}

# Toutes les communes AMP hors Marseille (source : geo.api.gouv.fr/epcis/200054807/communes)
# Format : code INSEE → nom de commune
AMP_COMMUNE_NAMES: dict[str, str] = {
    "13001": "Aix-en-Provence",
    "13002": "Allauch",
    "13003": "Alleins",
    "13005": "Aubagne",
    "13007": "Auriol",
    "13008": "Aurons",
    "13009": "La Barben",
    "13012": "Beaurecueil",
    "13013": "Belcodène",
    "13014": "Berre-l'Étang",
    "13015": "Bouc-Bel-Air",
    "13016": "La Bouilladisse",
    "13019": "Cabriès",
    "13020": "Cadolive",
    "13021": "Carry-le-Rouet",
    "13022": "Cassis",
    "13023": "Ceyreste",
    "13024": "Charleval",
    "13025": "Châteauneuf-le-Rouge",
    "13026": "Châteauneuf-les-Martigues",
    "13028": "La Ciotat",
    "13029": "Cornillon-Confoux",
    "13030": "Cuges-les-Pins",
    "13031": "La Destrousse",
    "13032": "Éguilles",
    "13033": "Ensuès-la-Redonne",
    "13035": "Eyguières",
    "13037": "La Fare-les-Oliviers",
    "13039": "Fos-sur-Mer",
    "13040": "Fuveau",
    "13041": "Gardanne",
    "13042": "Gémenos",
    "13043": "Gignac-la-Nerthe",
    "13044": "Grans",
    "13046": "Gréasque",
    "13047": "Istres",
    "13048": "Jouques",
    "13049": "Lamanon",
    "13050": "Lambesc",
    "13051": "Lançon-Provence",
    "13053": "Mallemort",
    "13054": "Marignane",
    "13056": "Martigues",
    "13059": "Meyrargues",
    "13060": "Meyreuil",
    "13062": "Mimet",
    "13063": "Miramas",
    "13069": "Pélissanne",
    "13070": "La Penne-sur-Huveaune",
    "13071": "Les Pennes-Mirabeau",
    "13072": "Peynier",
    "13073": "Peypin",
    "13074": "Peyrolles-en-Provence",
    "13075": "Plan-de-Cuques",
    "13077": "Port-de-Bouc",
    "13078": "Port-Saint-Louis-du-Rhône",
    "13079": "Puyloubier",
    "13080": "Le Puy-Sainte-Réparade",
    "13081": "Rognac",
    "13082": "Rognes",
    "13084": "La Roque-d'Anthéron",
    "13085": "Roquefort-la-Bédoule",
    "13086": "Roquevaire",
    "13087": "Rousset",
    "13088": "Le Rove",
    "13090": "Saint-Antonin-sur-Bayon",
    "13091": "Saint-Cannat",
    "13092": "Saint-Chamas",
    "13093": "Saint-Estève-Janson",
    "13095": "Saint-Marc-Jaumegarde",
    "13098": "Saint-Mitre-les-Remparts",
    "13099": "Saint-Paul-lès-Durance",
    "13101": "Saint-Savournin",
    "13102": "Saint-Victoret",
    "13103": "Salon-de-Provence",
    "13104": "Sausset-les-Pins",
    "13105": "Sénas",
    "13106": "Septèmes-les-Vallons",
    "13107": "Simiane-Collongue",
    "13109": "Le Tholonet",
    "13110": "Trets",
    "13111": "Vauvenargues",
    "13112": "Velaux",
    "13113": "Venelles",
    "13114": "Ventabren",
    "13115": "Vernègues",
    "13117": "Vitrolles",
    "13118": "Coudoux",
    "13119": "Carnoux-en-Provence",
    "83120": "Saint-Zacharie",
    "84089": "Pertuis",
}

# Table complète code INSEE → nom commune (arrondissements + communes AMP)
CODE_TO_COMMUNE: dict[str, str] = {**_MARSEILLE_NAMES, **AMP_COMMUNE_NAMES}

# Listes de codes seuls (rétrocompatibilité)
AMP_COMMUNE_CODES: list[str] = list(AMP_COMMUNE_NAMES.keys())

# Liste complète de tous les codes à charger (arrondissements Marseille + communes AMP)
ALL_AMP_CODES: list[str] = MARSEILLE_CODES + AMP_COMMUNE_CODES

# Codes CS2 pâturables retenus pour le calcul de pct_prairie — OCS GE v2
# pct_prairie = surface totale de ces couvertures / surface parcelle × 100
# CS2.1.1.2 (conifères) et CS2.1.3 (vignes/oliviers) volontairement exclus.
PRAIRIE_CS_PREFIXES = (
    "CS2.2.1",    # Formations herbacées (prairies, pelouses) — cœur de cible
    "CS2.1.2",    # Formations arbustives (landes, maquis, garrigue)
    "CS2.1.1.1",  # Peuplement de feuillus (pâturage sous-bois)
    "CS2.1.1.3",  # Peuplement mixte
    "CS2.2.2",    # Autres formations non ligneuses
)


def _read_cadastre_file(path: Path, code: str) -> gpd.GeoDataFrame:
    with gzip.open(path, "rb") as f:
        buf = io.BytesIO(f.read())
    gdf = gpd.read_file(buf, engine="pyogrio")
    gdf["code_commune"] = code
    # Nom de commune systématiquement renseigné depuis la table INSEE
    # → couvre 100% des parcelles, y compris celles sans propriétaire moral
    gdf["nom_commune_cadastre"] = CODE_TO_COMMUNE.get(code, code)
    return gdf


def load_cadastre(cadastre_dir: str | Path) -> gpd.GeoDataFrame:
    """Charge et fusionne les parcelles cadastrales de toutes les communes AMP.

    Parameters
    ----------
    cadastre_dir:
        Dossier contenant les fichiers ``parcelles-<code>.json.gz``.
        Téléchargez-les avec : python scripts/download_cadastre_amp.py

    Returns
    -------
    GeoDataFrame en WGS84 (EPSG:4326).
    """
    cadastre_dir = Path(cadastre_dir)
    gdfs = []
    missing = []
    tasks: list[tuple[Path, str]] = []
    for code in ALL_AMP_CODES:
        path = cadastre_dir / f"parcelles-{code}.json.gz"
        if not path.exists():
            missing.append(path.name)
            continue
        tasks.append((path, code))

    if tasks:
        max_workers = min(8, os.cpu_count() or 4)
        progress = tqdm(total=len(tasks), desc="Chargement cadastre", unit="fichier") if tqdm else None
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_read_cadastre_file, path, code) for path, code in tasks]
            for fut in as_completed(futures):
                gdfs.append(fut.result())
                if progress:
                    progress.update(1)
        if progress:
            progress.close()

    if missing:
        print(f"  ⚠ {len(missing)} fichier(s) manquant(s) — lancez : python scripts/download_cadastre_amp.py")
        for name in missing[:5]:
            print(f"    - {name}")
        if len(missing) > 5:
            print(f"    ... et {len(missing) - 5} autres")

    if not gdfs:
        raise FileNotFoundError(
            f"Aucune parcelle cadastrale trouvée dans {cadastre_dir}\n"
            "Lancez : python scripts/download_cadastre_amp.py"
        )

    merged = pd.concat(gdfs, ignore_index=True)
    result = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")
    loaded = len(ALL_AMP_CODES) - len(missing)
    print(f"  → {len(result):,} parcelles cadastrales chargées ({loaded}/{len(ALL_AMP_CODES)} communes/arrondissements AMP)")
    return result


def load_cadastre_dept(dept: str, cadastre_dir: str | Path) -> gpd.GeoDataFrame:
    """Charge les parcelles cadastrales d'un département quelconque.

    Supporte les deux formats produits par download_paca.py :
      - cadastre-{code}-parcelles.json      (décompressé)
      - parcelles-{code}.json.gz            (compressé, format AMP)

    Parameters
    ----------
    dept:
        Code département (ex: "04", "83").
    cadastre_dir:
        Dossier contenant les fichiers cadastraux.

    Returns
    -------
    GeoDataFrame en WGS84 (EPSG:4326).
    """
    import json as _json

    cadastre_dir = Path(cadastre_dir)
    gdfs = []

    # Chercher tous les fichiers du département (les deux formats)
    patterns = [
        f"cadastre-{dept}*-parcelles.json",
        f"parcelles-{dept}*.json.gz",
    ]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(cadastre_dir.glob(pattern)))

    if not files:
        raise FileNotFoundError(
            f"Aucun fichier cadastral trouvé pour le dept {dept} dans {cadastre_dir}\n"
            f"Lancez : python scripts/download_paca.py --dept {dept}"
        )

    def _read_file(path: Path) -> gpd.GeoDataFrame:
        if path.suffix == ".gz":
            with gzip.open(path, "rb") as f:
                buf = io.BytesIO(f.read())
            gdf = gpd.read_file(buf, engine="pyogrio")
        else:
            gdf = gpd.read_file(path, engine="pyogrio")
        # Extraire le code commune depuis le nom de fichier
        # cadastre-{code}-parcelles.json  → code = stem sans "cadastre-" et "-parcelles"
        stem = path.stem  # ex: "cadastre-04001-parcelles"
        if stem.startswith("cadastre-"):
            code = stem.split("-")[1]
        else:
            code = stem.split("-")[1] if "-" in stem else dept
        gdf["code_commune"] = code
        gdf["nom_commune_cadastre"] = CODE_TO_COMMUNE.get(code, code)
        return gdf

    max_workers = min(8, os.cpu_count() or 4)
    progress = tqdm(total=len(files), desc=f"Chargement cadastre dept {dept}", unit="fichier") if tqdm else None
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_read_file, f) for f in files]
        for fut in as_completed(futures):
            gdfs.append(fut.result())
            if progress:
                progress.update(1)
    if progress:
        progress.close()

    merged = pd.concat(gdfs, ignore_index=True)
    result = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")
    print(f"  → {len(result):,} parcelles cadastrales chargées ({len(files)} fichiers, dept {dept})")
    return result


def join_owners_to_cadastre(
    cadastre: gpd.GeoDataFrame,
    owners: gpd.GeoDataFrame,
    include_without_owner: bool = False,
) -> gpd.GeoDataFrame:
    """Jointure des propriétaires (points) sur les parcelles cadastrales.

    Utilise une jointure spatiale point-dans-polygone. Par défaut, seules les
    parcelles contenant au moins un point propriétaire sont conservées.

    Parameters
    ----------
    cadastre:
        GeoDataFrame des parcelles (WGS84, polygones).
    owners:
        GeoDataFrame des propriétaires (WGS84, points) avec colonnes
        ``denomination``, ``siren``, ``nom_commune``.

    Returns
    -------
    GeoDataFrame des parcelles avec colonnes propriétaires ajoutées.
    """
    if cadastre.crs.to_epsg() != 4326:
        cadastre = cadastre.to_crs("EPSG:4326")
    if owners.crs.to_epsg() != 4326:
        owners = owners.to_crs("EPSG:4326")

    owner_cols = [c for c in ["denomination", "siren", "nom_commune", "proprietaire_type"] if c in owners.columns]

    print(f"  → jointure point-dans-polygone ({len(cadastre):,} parcelles × {len(owners):,} propriétaires)…")
    join_mode = "left" if include_without_owner else "inner"
    result = gpd.sjoin(cadastre, owners[owner_cols + ["geometry"]], how=join_mode, predicate="contains")

    if "index_right" in result.columns:
        result = result.drop(columns=["index_right"])
    result = result.loc[:, ~result.columns.duplicated()]

    # Garder une seule entrée par parcelle (le premier propriétaire trouvé)
    id_col = "id" if "id" in result.columns else result.columns[0]
    result = result.drop_duplicates(subset=[id_col])

    # nom_commune : utiliser celui des personnes morales si présent,
    # sinon fallback sur nom_commune_cadastre (issu du code INSEE, 100% renseigné)
    if "nom_commune" in result.columns and "nom_commune_cadastre" in result.columns:
        result["nom_commune"] = result["nom_commune"].where(
            result["nom_commune"].notna(),
            result["nom_commune_cadastre"],
        )
        result = result.drop(columns=["nom_commune_cadastre"])
    elif "nom_commune_cadastre" in result.columns:
        result = result.rename(columns={"nom_commune_cadastre": "nom_commune"})

    if include_without_owner:
        count_with_owner = result["denomination"].notna().sum() if "denomination" in result.columns else 0
        print(f"  → {len(result):,} parcelles, dont {count_with_owner:,} avec propriétaire identifié")
    else:
        print(f"  → {len(result):,} parcelles avec propriétaire identifié")
    return result


def add_prairie_ratio(
    cadastre: gpd.GeoDataFrame,
    ocsge: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Calcule ``pct_prairie`` et le détail CS pour chaque parcelle cadastrale.

    Intersection spatiale parcelle × OCS GE :
        pct_prairie = surface(CS2.2.1 ∩ parcelle) / surface(parcelle) × 100
        cs_detail   = JSON {code_cs: surface_m2} pour chaque type présent

    Parameters
    ----------
    cadastre:
        GeoDataFrame des parcelles en Lambert 93 ou WGS84.
    ocsge:
        GeoDataFrame OCS GE (toutes zones pâturables) en Lambert 93.

    Returns
    -------
    GeoDataFrame des parcelles avec colonnes ``pct_prairie`` et ``cs_detail``.
    """
    import json as _json
    import warnings

    # Travailler en Lambert 93 pour les surfaces
    cad_proj = cadastre.to_crs("EPSG:2154").copy()
    ocs_proj = ocsge.to_crs("EPSG:2154").copy()

    # Marquer les zones prairie et conserver code_cs
    is_prairie = ocs_proj["code_cs"].astype(str).apply(
        lambda c: any(c.startswith(p) for p in PRAIRIE_CS_PREFIXES)
    )
    ocs_proj["is_prairie"] = is_prairie

    # Ajouter index parcelle
    cad_proj = cad_proj.reset_index(drop=True)
    cad_proj["_idx"] = cad_proj.index
    cad_proj["_area"] = cad_proj.geometry.area  # surface parcelle en m²

    print("  → intersection spatiale parcelles × OCS GE pour calcul % prairie…")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inter = gpd.overlay(
            cad_proj[["_idx", "_area", "geometry"]],
            ocs_proj[["is_prairie", "code_cs", "geometry"]],
            how="intersection",
        )

    inter["inter_area"] = inter.geometry.area

    # Surface prairie (CS2.2.1) par parcelle → pct_prairie
    prairie_ocs = (
        inter[inter["is_prairie"]]
        .groupby("_idx")["inter_area"].sum()
        .rename("prairie_m2")
    )
    parcel_areas = cad_proj.set_index("_idx")["_area"]
    pct = (prairie_ocs / parcel_areas * 100).clip(0, 100).round(1)
    cad_proj["prairie_m2"] = cad_proj["_idx"].map(prairie_ocs)
    cad_proj["pct_prairie"] = cad_proj["_idx"].map(pct)

    # Détail surfaces par code CS → colonne cs_detail (JSON string)
    # ex: {"CS2.2.1": 1200, "CS2.1.2": 450}
    cs_agg = (
        inter.groupby(["_idx", "code_cs"])["inter_area"]
        .sum()
        .round(0)
        .astype(int)
        .reset_index()
    )
    if cs_agg.empty:
        cs_by_parcel = pd.Series(dtype=object)
    else:
        cs_by_parcel = cs_agg.groupby("_idx").apply(
            lambda g: _json.dumps({row["code_cs"]: row["inter_area"] for _, row in g.iterrows()})
        )
        # pandas >= 2.2 peut retourner un DataFrame si include_groups=False n'est pas spécifié
        if isinstance(cs_by_parcel, pd.DataFrame):
            cs_by_parcel = pd.Series(dtype=object)
    cad_proj["cs_detail"] = cad_proj["_idx"].map(cs_by_parcel).fillna("{}")

    cad_proj = cad_proj.drop(columns=["_idx", "_area"])

    # Reprojeter dans le CRS original
    result = cad_proj.to_crs(cadastre.crs)
    prairie_mean = result["pct_prairie"].dropna().mean()
    if prairie_mean == prairie_mean:
        print(f"  → % prairie moyen : {prairie_mean:.1f}%")
    else:
        print("  → % prairie moyen : N/A (aucune donnée prairie)")
    return result
