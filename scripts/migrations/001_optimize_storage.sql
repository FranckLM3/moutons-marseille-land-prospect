-- ============================================================
-- Migration 001 — Optimisation stockage Supabase (plan gratuit)
-- À coller et exécuter dans le SQL Editor du dashboard Supabase
-- https://supabase.com/dashboard/project/dfgofyyhyqykpsutpnat/sql/new
--
-- Gains estimés :
--   - DROP area_ha           :  ~3 MB
--   - DROP prairie_m2        :  ~3 MB
--   - Simplification geom    : ~50 MB (geom) + ~100 MB (index GIST)
--   Total visé : ~150 MB libérés
-- ============================================================

-- ── Étape 0 : état initial ────────────────────────────────────────────────
SELECT
  pg_size_pretty(pg_total_relation_size('parcelles'))    AS taille_totale,
  pg_size_pretty(pg_relation_size('parcelles'))          AS taille_donnees,
  pg_size_pretty(pg_indexes_size('parcelles'))           AS taille_index,
  count(*)                                               AS nb_lignes
FROM parcelles;

-- ── Étape 1 : supprimer les colonnes redondantes ──────────────────────────
-- area_ha = area_m2 / 10000  (calculé côté client ou dans le RPC)
-- prairie_m2 = area_m2 * pct_prairie / 100  (idem)
ALTER TABLE parcelles DROP COLUMN IF EXISTS area_ha;
ALTER TABLE parcelles DROP COLUMN IF EXISTS prairie_m2;

-- ── Étape 2a : relâcher le type de colonne ───────────────────────────────
-- ST_SimplifyPreserveTopology peut produire un MultiPolygon quand deux parties
-- d'un polygone se séparent à la simplification. La contrainte Polygon(4326)
-- rejette ces géométries → on passe en geometry(Geometry, 4326).
ALTER TABLE parcelles
  ALTER COLUMN geom TYPE geometry(Geometry, 4326)
  USING geom::geometry(Geometry, 4326);

-- ── Étape 2b : simplifier les géométries ─────────────────────────────────
-- Tolérance 0.00008° ≈ 8 m en WGS84 à lat 43°N → suffisant pour visualisation web
-- ST_ReducePrecision : arrondi à 5 décimales (≈ 1 m), réduit le stockage WKB
-- Exécuter en 4 lots pour éviter timeout (chaque lot ~90K lignes, ~30-60s)

-- Lot 1
UPDATE parcelles
SET geom = ST_MakeValid(
             ST_ReducePrecision(
               ST_SimplifyPreserveTopology(ST_MakeValid(geom), 0.00008),
               0.00001
             )
           )
WHERE id <= (SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY id) FROM parcelles);

-- Lot 2
UPDATE parcelles
SET geom = ST_MakeValid(
             ST_ReducePrecision(
               ST_SimplifyPreserveTopology(ST_MakeValid(geom), 0.00008),
               0.00001
             )
           )
WHERE id > (SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY id) FROM parcelles)
  AND id <= (SELECT percentile_disc(0.50) WITHIN GROUP (ORDER BY id) FROM parcelles);

-- Lot 3
UPDATE parcelles
SET geom = ST_MakeValid(
             ST_ReducePrecision(
               ST_SimplifyPreserveTopology(ST_MakeValid(geom), 0.00008),
               0.00001
             )
           )
WHERE id > (SELECT percentile_disc(0.50) WITHIN GROUP (ORDER BY id) FROM parcelles)
  AND id <= (SELECT percentile_disc(0.75) WITHIN GROUP (ORDER BY id) FROM parcelles);

-- Lot 4
UPDATE parcelles
SET geom = ST_MakeValid(
             ST_ReducePrecision(
               ST_SimplifyPreserveTopology(ST_MakeValid(geom), 0.00008),
               0.00001
             )
           )
WHERE id > (SELECT percentile_disc(0.75) WITHIN GROUP (ORDER BY id) FROM parcelles);

-- ── Étape 3 : mettre à jour le RPC parcelles_by_communes ─────────────────
-- Il faut recalculer prairie_m2 à la volée puisqu'on a supprimé la colonne
CREATE OR REPLACE FUNCTION parcelles_by_communes(
  communes    text[],
  min_prairie float DEFAULT 0
)
RETURNS TABLE (
  id                text,
  area_m2           int,
  denomination      text,
  siren             text,
  nom_commune       text,
  pct_prairie       float,
  prairie_m2        float,
  cs_detail         jsonb,
  proprietaire_type text,
  geojson           text
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    id,
    area_m2,
    denomination,
    siren,
    nom_commune,
    pct_prairie,
    round(area_m2 * pct_prairie / 100)::float AS prairie_m2,
    cs_detail,
    proprietaire_type,
    ST_AsGeoJSON(geom, 6)  AS geojson
  FROM parcelles
  WHERE nom_commune = ANY(communes)
    AND pct_prairie >= min_prairie
  LIMIT 10000;
$$;

-- ── Étape 4 : reconstruire l'index GIST (plus compact après simplification) ─
REINDEX INDEX CONCURRENTLY parcelles_geom_idx;

-- ── Étape 5 : récupérer l'espace libéré ───────────────────────────────────
-- VACUUM FULL bloque les lectures, préférer en période creuse
VACUUM FULL ANALYZE parcelles;

-- ── Étape 6 : vérification post-migration ────────────────────────────────
SELECT
  pg_size_pretty(pg_total_relation_size('parcelles'))    AS taille_totale,
  pg_size_pretty(pg_relation_size('parcelles'))          AS taille_donnees,
  pg_size_pretty(pg_indexes_size('parcelles'))           AS taille_index,
  count(*)                                               AS nb_lignes,
  round(avg(ST_NPoints(geom)))                           AS avg_points_par_geom
FROM parcelles;
