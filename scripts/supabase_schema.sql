-- ============================================================
-- Table parcelles — stockage des zones pâturables dans Supabase
-- À exécuter dans l'éditeur SQL Supabase (une seule fois)
-- ============================================================

-- 1. Activer PostGIS (si pas encore fait)
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. Table principale
CREATE TABLE IF NOT EXISTS public.parcelles (
    id           TEXT PRIMARY KEY,           -- identifiant cadastral unique
    area_m2      DOUBLE PRECISION,
    area_ha      DOUBLE PRECISION,
    denomination TEXT,
    siren        TEXT,
    nom_commune  TEXT NOT NULL,
    pct_prairie  DOUBLE PRECISION,
    prairie_m2   DOUBLE PRECISION,
    cs_detail    JSONB,
    geom         GEOMETRY(Polygon, 4326) NOT NULL
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
CREATE OR REPLACE FUNCTION public.parcelles_by_communes(
    communes     TEXT[],
    min_prairie  DOUBLE PRECISION DEFAULT 0
)
RETURNS TABLE (
    id           TEXT,
    area_m2      DOUBLE PRECISION,
    area_ha      DOUBLE PRECISION,
    denomination TEXT,
    siren        TEXT,
    nom_commune  TEXT,
    pct_prairie  DOUBLE PRECISION,
    prairie_m2   DOUBLE PRECISION,
    cs_detail    JSONB,
    geojson      TEXT   -- geometry serialisée en GeoJSON pour Leaflet
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
        ST_AsGeoJSON(p.geom) AS geojson
    FROM public.parcelles p
    WHERE p.nom_commune = ANY(communes)
      AND (min_prairie = 0 OR COALESCE(p.prairie_m2, 0) >= min_prairie)
    ORDER BY p.prairie_m2 DESC NULLS LAST;
$$;

-- Accessible en lecture anonyme
GRANT EXECUTE ON FUNCTION public.parcelles_by_communes TO anon, authenticated;

-- ============================================================
-- Fonction RPC : liste_communes
-- Retourne la liste triée des communes distinctes dans la table
-- ============================================================
CREATE OR REPLACE FUNCTION public.liste_communes()
RETURNS TABLE (nom_commune TEXT)
LANGUAGE SQL STABLE
AS $$
    SELECT DISTINCT p.nom_commune
    FROM public.parcelles p
    WHERE p.nom_commune IS NOT NULL
    ORDER BY p.nom_commune;
$$;

GRANT EXECUTE ON FUNCTION public.liste_communes TO anon, authenticated;

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
             pct_prairie, prairie_m2, cs_detail, geom)
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
            ST_GeomFromGeoJSON(r->>'geom_geojson')
        )
        ON CONFLICT (id) DO UPDATE SET
            area_m2      = EXCLUDED.area_m2,
            area_ha      = EXCLUDED.area_ha,
            denomination = EXCLUDED.denomination,
            siren        = EXCLUDED.siren,
            nom_commune  = EXCLUDED.nom_commune,
            pct_prairie  = EXCLUDED.pct_prairie,
            prairie_m2   = EXCLUDED.prairie_m2,
            cs_detail    = EXCLUDED.cs_detail,
            geom         = EXCLUDED.geom;
        inserted := inserted + 1;
    END LOOP;
    RETURN inserted;
END;
$$;

GRANT EXECUTE ON FUNCTION public.upsert_parcelles_batch TO service_role;
