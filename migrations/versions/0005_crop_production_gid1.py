"""Add crop_production_pfaf_gid1 (production per basin-state slice).

`crop_production_pfaf` is keyed by `pfaf_id` only, so a state search had to
approximate basin-state production by area-weighting the full-basin total.
The new reference CSV (`all_crops_pfaf_melt_gid_1.csv`) carries `gid_1`, so
production can be looked up directly per admin-1 slice.

The basin-level table is left in place: summing this table over `gid_1`
reproduces it, and keeping both allows verifying the new data before the
old one is retired.

Like 0002, this runs as the Aurora superuser, so ownership is handed to the
application role afterwards (see 0003): `load_reference_data --data-only`
issues TRUNCATE, which requires ownership rather than plain privileges.

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-13

"""
import os

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

APP_ROLE = os.environ.get("APP_DB_ROLE", "aqueduct_flood")


def _transfer_ownership(role: str) -> str:
    """Hand the new table to `role`, no-op when the role does not exist
    (local dev connects as `postgres` and has no app role)."""
    return f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                ALTER TABLE crop_production_pfaf_gid1 OWNER TO "{role}";
            END IF;
        END
        $$;
        """


def upgrade():
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS crop_production_pfaf_gid1 (
            pfaf_id          BIGINT NOT NULL,
            gid_1            TEXT   NOT NULL,
            commodity        TEXT   NOT NULL,
            basin_production DOUBLE PRECISION,
            irrigation       TEXT   NOT NULL,
            PRIMARY KEY (pfaf_id, gid_1, commodity, irrigation)
        )
        """
    )
    # Matches the analysis join order: commodity + irrigation are fixed per
    # input location, then the (pfaf_id, gid_1) slices are probed.
    op.execute(
        "CREATE INDEX IF NOT EXISTS crop_prod_gid1_lookup_idx "
        "ON crop_production_pfaf_gid1 (commodity, irrigation, pfaf_id, gid_1)"
    )
    op.execute(_transfer_ownership(APP_ROLE.replace("'", "''")))


def downgrade():
    op.execute("DROP INDEX IF EXISTS crop_prod_gid1_lookup_idx")
    op.execute("DROP TABLE IF EXISTS crop_production_pfaf_gid1")
