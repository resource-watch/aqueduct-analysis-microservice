-- ============================================================================
-- Enable PostGIS — one-time, per environment (staging, then production)
--
-- MUST be run by a role with rds_superuser (e.g. the RDS master user).
-- The application role (aqueduct_flood) cannot run CREATE EXTENSION on
-- PostgreSQL 11 (no trusted extensions before PG 13).
--
-- Target databases:
--   staging:    aqueduct_flood @ the staging Aurora cluster
--   production: aqueduct_flood @ the production Aurora cluster
--
-- How to run (through an SSM port-forward tunnel on localhost:5432):
--   psql "postgresql://<master_user>@localhost:5432/aqueduct_flood" \
--        -f migrations/admin/001_enable_postgis.sql
--
-- After this, the application-level migrations can be applied with the
-- regular app credentials:  alembic upgrade head
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- Verification: both statements must succeed and print a version.
SELECT extversion AS postgis_extension_version
FROM pg_extension
WHERE extname = 'postgis';

SELECT postgis_full_version();
