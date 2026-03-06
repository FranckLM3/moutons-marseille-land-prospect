-- ============================================================
-- Table parcelles — stockage des zones pâturables dans Supabase
-- À exécuter dans l'éditeur SQL Supabase (une seule fois)
-- ============================================================

-- 1. Activer PostGIS (si pas encore fait)
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. Table principale
CREATE TABLE IF NOT EXISTS public.parcelles (
    id                TEXT PRIMARY KEY,           -- identifiant cadastral unique
    area_m2           DOUBLE PRECISION,
    area_ha           DOUBLE PRECISION,
    denomination      TEXT,
    siren             TEXT,
    nom_commune       TEXT NOT NULL,
    pct_prairie       DOUBLE PRECISION,
    prairie_m2        DOUBLE PRECISION,
    cs_detail         JSONB,
    proprietaire_type TEXT,                       -- 'public' | 'semi-public' | 'privé' | 'indéterminé'
    geom              GEOMETRY(Polygon, 4326) NOT NULL
);

-- 3. Index spatial (essentiel pour les requêtes bbox)
CREATE INDEX IF NOT EXISTS parcelles_geom_idx
    ON public.parcelles USING GIST (geom);

-- 4. Index sur nom_commune (filtre par commune)
CREATE INDEX IF NOT EXISTS parcelles_commune_idx
    ON public.parcelles (nom_commune);

-- 5. Index sur prairie_m2 (filtre surface min)
CREATE INDEX IF NOT EXISTS parcelles_prairie_idx
    ON public.parcelles (prairie_m2);

-- 5b. Migration : ajouter proprietaire_type si la table existe déjà
ALTER TABLE public.parcelles ADD COLUMN IF NOT EXISTS proprietaire_type TEXT;

-- 6. RLS — lecture publique (anon), écriture interdite
ALTER TABLE public.parcelles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "parcelles_select_public"
    ON public.parcelles FOR SELECT
    TO anon, authenticated
    USING (true);

-- Pas de politique INSERT/UPDATE/DELETE → seul le service role peut écrire
-- (utilisé uniquement par le script d'import Python avec la clé service)

-- ============================================================
-- Fonction RPC : parcelles_by_communes
-- Retourne les parcelles pour une liste de communes + filtre surface min
-- Appelée depuis app.js via supabase.rpc(...)
-- ============================================================
-- DROP requis si la signature de retour change (PostgreSQL ne supporte pas
-- CREATE OR REPLACE avec un RETURNS TABLE différent)
DROP FUNCTION IF EXISTS public.parcelles_by_communes(text[], double precision);

CREATE OR REPLACE FUNCTION public.parcelles_by_communes(
    communes     TEXT[],
    min_prairie  DOUBLE PRECISION DEFAULT 0
)
RETURNS TABLE (
    id                TEXT,
    area_m2           DOUBLE PRECISION,
    area_ha           DOUBLE PRECISION,
    denomination      TEXT,
    siren             TEXT,
    nom_commune       TEXT,
    pct_prairie       DOUBLE PRECISION,
    prairie_m2        DOUBLE PRECISION,
    cs_detail         JSONB,
    proprietaire_type TEXT,
    geojson           TEXT   -- geometry serialisée en GeoJSON pour Leaflet
)
LANGUAGE SQL STABLE
AS $$
    SELECT
        p.id,
        p.area_m2,
        p.area_ha,
        p.denomination,
        p.siren,
        p.nom_commune,
        p.pct_prairie,
        p.prairie_m2,
        p.cs_detail,
        p.proprietaire_type,
        ST_AsGeoJSON(p.geom) AS geojson
    FROM public.parcelles p
    WHERE p.nom_commune = ANY(communes)
      AND (min_prairie = 0 OR COALESCE(p.prairie_m2, 0) >= min_prairie)
    ORDER BY p.prairie_m2 DESC NULLS LAST;
$$;

-- Accessible en lecture anonyme
GRANT EXECUTE ON FUNCTION public.parcelles_by_communes TO anon, authenticated;

-- ============================================================
-- Fonction RPC : parcelles_dans_corridor
-- Retourne les parcelles dont le centroïde est à moins de
-- radius_km km d'une polyligne GeoJSON (tracé d'itinéraire).
-- Appelée depuis app.js > computeRoute()
-- ============================================================
DROP FUNCTION IF EXISTS public.parcelles_dans_corridor(text, double precision, double precision);

CREATE OR REPLACE FUNCTION public.parcelles_dans_corridor(
    route_geojson  TEXT,            -- GeoJSON LineString (coordonnées du tracé ORS)
    radius_km      DOUBLE PRECISION DEFAULT 2,
    min_prairie    DOUBLE PRECISION DEFAULT 0
)
RETURNS TABLE (
    id                TEXT,
    area_m2           DOUBLE PRECISION,
    area_ha           DOUBLE PRECISION,
    denomination      TEXT,
    siren             TEXT,
    nom_commune       TEXT,
    pct_prairie       DOUBLE PRECISION,
    prairie_m2        DOUBLE PRECISION,
    cs_detail         JSONB,
    proprietaire_type TEXT,
    geojson           TEXT
)
LANGUAGE SQL STABLE
AS $$
    SELECT
        p.id,
        p.area_m2,
        p.area_ha,
        p.denomination,
        p.siren,
        p.nom_commune,
        p.pct_prairie,
        p.prairie_m2,
        p.cs_detail,
        p.proprietaire_type,
        ST_AsGeoJSON(p.geom) AS geojson
    FROM public.parcelles p
    WHERE (min_prairie = 0 OR COALESCE(p.prairie_m2, 0) >= min_prairie)
      AND ST_DWithin(
            ST_Centroid(p.geom)::geography,
            ST_GeomFromGeoJSON(route_geojson)::geography,
            radius_km * 1000
          )
    ORDER BY p.prairie_m2 DESC NULLS LAST;
$$;

GRANT EXECUTE ON FUNCTION public.parcelles_dans_corridor TO anon, authenticated;

-- ============================================================
-- Index sur nom_commune (accélère liste_communes et les filtres)
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_parcelles_nom_commune
  ON public.parcelles (nom_commune);

-- ============================================================
-- Materialized view : communes_list
-- Rafraîchie après chaque import via REFRESH MATERIALIZED VIEW.
-- Évite le DISTINCT sur 1,7M lignes à chaque appel RPC.
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS public.communes_list AS
  SELECT DISTINCT nom_commune
  FROM public.parcelles
  WHERE nom_commune IS NOT NULL
  ORDER BY nom_commune;

CREATE UNIQUE INDEX IF NOT EXISTS idx_communes_list_nom
  ON public.communes_list (nom_commune);

GRANT SELECT ON public.communes_list TO anon, authenticated;

-- ============================================================
-- Fonction RPC : liste_communes
-- Lit depuis la materialized view (instantané)
-- ============================================================
DROP FUNCTION IF EXISTS public.liste_communes();
CREATE OR REPLACE FUNCTION public.liste_communes()
RETURNS TABLE (nom_commune TEXT)
LANGUAGE SQL STABLE
AS $$
    SELECT nom_commune FROM public.communes_list ORDER BY nom_commune;
$$;

GRANT EXECUTE ON FUNCTION public.liste_communes TO anon, authenticated;

-- ============================================================
-- Fonction RPC : refresh_communes_list
-- Appelée par le script d'import après chaque batch.
-- Nécessite les droits service_role.
-- ============================================================
CREATE OR REPLACE FUNCTION public.refresh_communes_list()
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.communes_list;
END;
$$;

GRANT EXECUTE ON FUNCTION public.refresh_communes_list TO service_role;

-- ============================================================
-- Fonction RPC : upsert_parcelles_batch
-- Utilisée par le script d'import Python.
-- Accepte un tableau JSONB de rows, chacun avec un champ
-- "geom_geojson" (GeoJSON string) qui est converti en geometry.
-- ============================================================
CREATE OR REPLACE FUNCTION public.upsert_parcelles_batch(rows JSONB)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    r        JSONB;
    inserted INT := 0;
BEGIN
    FOR r IN SELECT * FROM jsonb_array_elements(rows)
    LOOP
        INSERT INTO public.parcelles
            (id, area_m2, area_ha, denomination, siren, nom_commune,
             pct_prairie, prairie_m2, cs_detail, proprietaire_type, geom)
        VALUES (
            r->>'id',
            (r->>'area_m2')::DOUBLE PRECISION,
            (r->>'area_ha')::DOUBLE PRECISION,
            r->>'denomination',
            r->>'siren',
            r->>'nom_commune',
            (r->>'pct_prairie')::DOUBLE PRECISION,
            (r->>'prairie_m2')::DOUBLE PRECISION,
            (r->'cs_detail'),
            r->>'proprietaire_type',
            ST_GeomFromGeoJSON(r->>'geom_geojson')
        )
        ON CONFLICT (id) DO UPDATE SET
            area_m2           = EXCLUDED.area_m2,
            area_ha           = EXCLUDED.area_ha,
            denomination      = EXCLUDED.denomination,
            siren             = EXCLUDED.siren,
            nom_commune       = EXCLUDED.nom_commune,
            pct_prairie       = EXCLUDED.pct_prairie,
            prairie_m2        = EXCLUDED.prairie_m2,
            cs_detail         = EXCLUDED.cs_detail,
            proprietaire_type = EXCLUDED.proprietaire_type,
            geom              = EXCLUDED.geom;
        inserted := inserted + 1;
    END LOOP;
    RETURN inserted;
END;
$$;

GRANT EXECUTE ON FUNCTION public.upsert_parcelles_batch TO service_role;
