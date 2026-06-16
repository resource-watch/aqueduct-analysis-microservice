"""Transfer ownership of the reference tables to the application role.

Migrations 0001/0002 are run by a superuser (e.g. `postgres` on Aurora,
per the staging/production runbook), so the tables created in 0002 are
owned by that superuser. The application connects as a separate, less
privileged role (`aqueduct_flood`), which therefore has no access and
fails at query time with:

    psycopg2.errors.InsufficientPrivilege: permission denied for table ...

Plain SELECT/INSERT/TRUNCATE grants are not sufficient for the loader:
`load_reference_data --data-only` issues `TRUNCATE ... RESTART IDENTITY`,
which resets the owned sequence and therefore requires the caller to
*own* the table and its sequence, not merely hold TRUNCATE privilege:

    psycopg2.errors.InsufficientPrivilege: must be owner of sequence
    aq_basins_raw_id_seq

So this migration reassigns ownership of every reference table — and the
SERIAL-backed sequences from 0002 — to the application role. Ownership
implies all privileges, so the read path and the data-only loader both
work when connecting as that role.

The role name defaults to `aqueduct_flood` but can be overridden with
the APP_DB_ROLE environment variable. Everything is wrapped in a DO
block that no-ops if the role does not exist, so this migration is safe
to run in local dev (where the app connects as `postgres` and there is
no `aqueduct_flood` role).

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-16

"""
import os

from alembic import op

# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

APP_ROLE = os.environ.get("APP_DB_ROLE", "aqueduct_flood")

TABLES = ("aq_basins_raw", "gadm36_0", "sbtn_son_v2", "crop_production_pfaf")

# SERIAL primary keys in 0002 created these owned sequences.
SEQUENCES = ("aq_basins_raw_id_seq", "gadm36_0_id_seq")


def _owner_block(role: str, new_owner_sql: str) -> str:
    """Build a DO block that runs `new_owner_sql` only if `role` exists."""
    statements = []
    for tbl in TABLES:
        statements.append(f'ALTER TABLE {tbl} OWNER TO "{new_owner_sql}";')
    for seq in SEQUENCES:
        statements.append(f'ALTER SEQUENCE {seq} OWNER TO "{new_owner_sql}";')
    body = "\n                ".join(statements)
    return f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                {body}
            END IF;
        END
        $$;
        """


def upgrade():
    role = APP_ROLE.replace("'", "''")
    op.execute(_owner_block(role, role))


def downgrade():
    # Best-effort: hand the tables back to the role running this migration
    # (the superuser that originally owned them). Guarded on the app role's
    # existence so dev — where it was never reassigned — is a no-op.
    role = APP_ROLE.replace("'", "''")
    op.execute(
        f"""
        DO $$
        DECLARE
            current_owner text := current_user;
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                EXECUTE format('ALTER TABLE aq_basins_raw OWNER TO %I', current_owner);
                EXECUTE format('ALTER TABLE gadm36_0 OWNER TO %I', current_owner);
                EXECUTE format('ALTER TABLE sbtn_son_v2 OWNER TO %I', current_owner);
                EXECUTE format('ALTER TABLE crop_production_pfaf OWNER TO %I', current_owner);
                EXECUTE format('ALTER SEQUENCE aq_basins_raw_id_seq OWNER TO %I', current_owner);
                EXECUTE format('ALTER SEQUENCE gadm36_0_id_seq OWNER TO %I', current_owner);
            END IF;
        END
        $$;
        """
    )
