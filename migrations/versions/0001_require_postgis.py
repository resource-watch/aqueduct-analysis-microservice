"""Require the PostGIS extension.

The application role (aqueduct_flood) cannot CREATE EXTENSION on
PostgreSQL 11 — a DBA must run migrations/admin/001_enable_postgis.sql
once per environment first. This migration only verifies it happened,
so the schema migrations that depend on geometry types fail fast with
an actionable message instead of an obscure type error.

Revision ID: 0001
Revises:
Create Date: 2026-06-11

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    installed = conn.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'postgis'"
    ).fetchone()
    if installed is None:
        raise RuntimeError(
            "The 'postgis' extension is not installed in this database. "
            "It must be created by a role with rds_superuser before "
            "application migrations can run. Hand "
            "migrations/admin/001_enable_postgis.sql to a DBA, then re-run "
            "'alembic upgrade head'."
        )


def downgrade():
    # The extension is managed by the DBA, not by this migration chain.
    pass
