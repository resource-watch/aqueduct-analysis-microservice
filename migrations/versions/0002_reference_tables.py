"""Create the supply-chain + GADM reference tables.

Schemas intentionally match what ogr2ogr produced in the dev database
(verified via \\d there), so the loader can `ogr2ogr -append` /
`COPY` into them without schema drift:

  - aq_basins_raw        : Aqueduct basin polygons (one row per
                           pfaf_id x admin slice), geom column "geom".
  - gadm36_0             : GADM country polygons, geometry column MUST
                           be "the_geom" (queried by name in
                           aqueduct/services/gadm_service.py).
  - sbtn_son_v2          : SBTN indicators by pfaf_id.
  - crop_production_pfaf : production per (pfaf_id, commodity_code,
                           irrigation).

Data is NOT loaded here — run the loader afterwards:
  python -m aqueduct.services.supply_chain_data.load_reference_data --data-only

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-11

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        CREATE TABLE aq_basins_raw (
            id          SERIAL PRIMARY KEY,
            string_id   VARCHAR,
            aq30_id     DOUBLE PRECISION,
            shape_leng  DOUBLE PRECISION,
            string_id_1 VARCHAR,
            aq30_id_1   INTEGER,
            pfaf_id     INTEGER,
            gid_1       VARCHAR,
            aqid        INTEGER,
            gid_0       VARCHAR,
            name_0      VARCHAR,
            name_1      VARCHAR,
            area_km2    DOUBLE PRECISION,
            bws_raw     DOUBLE PRECISION,
            bws_score   DOUBLE PRECISION,
            bws_cat     DOUBLE PRECISION,
            bws_label   VARCHAR,
            bwd_raw     DOUBLE PRECISION,
            bwd_score   DOUBLE PRECISION,
            bwd_cat     DOUBLE PRECISION,
            bwd_label   VARCHAR,
            iav_raw     DOUBLE PRECISION,
            iav_score   DOUBLE PRECISION,
            iav_cat     DOUBLE PRECISION,
            iav_label   VARCHAR,
            sev_raw     DOUBLE PRECISION,
            sev_score   DOUBLE PRECISION,
            sev_cat     DOUBLE PRECISION,
            sev_label   VARCHAR,
            cep_raw     DOUBLE PRECISION,
            cep_score   DOUBLE PRECISION,
            cep_cat     DOUBLE PRECISION,
            cep_label   VARCHAR,
            geom        geometry(MultiPolygon, 4326)
        )
        """
    )
    op.execute(
        "CREATE INDEX aq_basins_raw_geom_geom_idx ON aq_basins_raw USING GIST (geom)"
    )
    op.execute("CREATE INDEX aq_basins_raw_pfaf_idx ON aq_basins_raw (pfaf_id)")
    op.execute("CREATE INDEX aq_basins_raw_iso_idx ON aq_basins_raw (gid_0)")
    op.execute(
        "CREATE INDEX aq_basins_raw_state_idx ON aq_basins_raw (name_0, name_1)"
    )

    op.execute(
        """
        CREATE TABLE gadm36_0 (
            id               SERIAL PRIMARY KEY,
            gid_0            VARCHAR,
            name_0           VARCHAR,
            coastal          BOOLEAN,
            geostore_staging VARCHAR,
            geostore_version VARCHAR,
            geostore_prod    VARCHAR,
            the_geom         geometry(MultiPolygon, 4326)
        )
        """
    )
    op.execute(
        "CREATE INDEX gadm36_0_the_geom_geom_idx ON gadm36_0 USING GIST (the_geom)"
    )

    op.execute(
        """
        CREATE TABLE sbtn_son_v2 (
            pfaf_id        BIGINT PRIMARY KEY,
            sbtn_quant_max DOUBLE PRECISION,
            sbtn_qual_max  DOUBLE PRECISION
        )
        """
    )

    op.execute(
        """
        CREATE TABLE crop_production_pfaf (
            pfaf_id          BIGINT NOT NULL,
            commodity_code   TEXT   NOT NULL,
            basin_production DOUBLE PRECISION,
            irrigation       TEXT   NOT NULL,
            PRIMARY KEY (pfaf_id, commodity_code, irrigation)
        )
        """
    )
    op.execute(
        "CREATE INDEX crop_prod_lookup_idx "
        "ON crop_production_pfaf (commodity_code, irrigation, pfaf_id)"
    )


def downgrade():
    op.execute("DROP TABLE IF EXISTS crop_production_pfaf")
    op.execute("DROP TABLE IF EXISTS sbtn_son_v2")
    op.execute("DROP TABLE IF EXISTS gadm36_0")
    op.execute("DROP TABLE IF EXISTS aq_basins_raw")
