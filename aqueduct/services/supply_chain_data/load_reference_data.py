"""Load Aqueduct food-supply-chain reference data into Postgres + PostGIS.

Tables:

  - aq_basins_raw         : raw rows from aq_data_supplychain.geojson
                            (one row per (pfaf_id, gid_1, aqid) admin slice).
                            This is the table the analysis endpoint queries
                            via ST_Intersects + GROUP BY pfaf_id.
  - sbtn_son_v2           : SBTN sustainability indicators by pfaf_id.
  - crop_production_pfaf  : production per (pfaf_id, commodity_code, irrigation).
  - gadm36_0              : GADM country polygons (geometry col `the_geom`).

Two modes:

  default (legacy/dev)    : the loader OWNS the schema — drops and
                            recreates each table, creates indexes, and
                            creates the postgis extension if missing.
                            Suitable for the local docker postgres.

  --data-only             : the schema is owned by Alembic migrations
                            (see migrations/versions/). The loader only
                            TRUNCATEs and refills the tables; it never
                            issues DDL and never touches the extension.
                            Use this against staging/production where the
                            app role has no CREATE EXTENSION privilege:

                              alembic upgrade head      # once, schema
                              python -m aqueduct.services.supply_chain_data.load_reference_data --data-only

Run inside the dev container (which has ogr2ogr + psycopg2):

    docker exec -it aqueduct_analysis-develop \
        python -m aqueduct.services.supply_chain_data.load_reference_data

Honors POSTGRES_URL from the environment (the value the Flask service uses).
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql

LOG = logging.getLogger("supply-chain-loader")

DEFAULT_DATA_DIR = Path("/data")
GEOJSON_FILE = "aq_data_supplychain.geojson"
SBTN_FILE = "SBTN_SON_V2.csv"
CROPS_FILE = "all_crops_pfaf_melt.csv"
GADM_FILE = "gadm36_0.gpkg"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help="Directory containing the three reference files (default: /data)",
    )
    p.add_argument(
        "--postgres-url",
        default=os.environ.get("POSTGRES_URL"),
        help="SQLAlchemy-style postgres URL (defaults to $POSTGRES_URL)",
    )
    p.add_argument("--skip-basins", action="store_true")
    p.add_argument("--skip-sbtn", action="store_true")
    p.add_argument("--skip-crops", action="store_true")
    p.add_argument("--skip-gadm", action="store_true")
    p.add_argument(
        "--data-only",
        action="store_true",
        help=(
            "Do not issue any DDL (no DROP/CREATE TABLE, no indexes, no "
            "CREATE EXTENSION). Tables must already exist — created by "
            "'alembic upgrade head'. Existing rows are TRUNCATEd and "
            "reloaded. Required for staging/prod where the app role "
            "cannot create extensions."
        ),
    )
    p.add_argument(
        "--only",
        choices=["basins", "sbtn", "crops", "gadm"],
        action="append",
        help=(
            "Limit the run to one or more datasets (repeatable). When set, "
            "all other tables are skipped."
        ),
    )
    return p.parse_args()


def normalize_pg_url(url: str | None) -> str:
    """Accept SQLAlchemy URL (postgresql+psycopg2://) and return libpq one."""
    if not url:
        raise SystemExit("POSTGRES_URL is not set")
    if url.startswith("postgresql+psycopg2://"):
        url = "postgresql://" + url[len("postgresql+psycopg2://"):]
    return url


def pg_kwargs(libpq_url: str) -> dict:
    p = urlparse(libpq_url)
    return {
        "host": p.hostname,
        "port": p.port or 5432,
        "user": p.username,
        "password": p.password,
        "dbname": p.path.lstrip("/"),
    }


def ensure_postgis(conn) -> None:
    """Make sure the PostGIS extension exists in the target database."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    conn.commit()
    LOG.info("PostGIS extension is present")


def _truncate(libpq_url: str, table: str) -> None:
    """TRUNCATE a migration-owned table before an -append reload."""
    conn = psycopg2.connect(**pg_kwargs(libpq_url))
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY").format(
                    sql.Identifier(table)
                )
            )
        conn.commit()
        LOG.info("truncated %s", table)
    finally:
        conn.close()


def _ogr2ogr_load(
    libpq_url: str,
    src_path: Path,
    table: str,
    geometry_name: str,
    data_only: bool,
    layer: str | None = None,
) -> None:
    """Bulk-load a vector file into Postgres via ogr2ogr.

    data_only=False (legacy/dev): -overwrite recreates the table with
    layer-creation options (geometry column name, FID, GIST index).

    data_only=True (staging/prod): the table already exists (created by
    Alembic). We TRUNCATE it, then -append into the existing schema.
    -unsetFid lets the SERIAL id column self-generate instead of reusing
    source feature ids.
    """
    if not src_path.exists():
        raise SystemExit(f"missing source file: {src_path}")

    p = urlparse(libpq_url)
    ogr_pg = (
        f"PG:host={p.hostname} port={p.port or 5432} "
        f"dbname={p.path.lstrip('/')} user={p.username} password={p.password}"
    )

    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        ogr_pg,
        str(src_path),
    ]
    if layer:
        cmd.append(layer)
    cmd += [
        "-nln", table,
        "-nlt", "PROMOTE_TO_MULTI",
        "-t_srs", "EPSG:4326",
        "--config", "PG_USE_COPY", "YES",
        "-progress",
    ]
    if data_only:
        _truncate(libpq_url, table)
        cmd += ["-append", "-unsetFid"]
    else:
        cmd += [
            "-overwrite",
            "-lco", f"GEOMETRY_NAME={geometry_name}",
            "-lco", "FID=id",
            "-lco", "PRECISION=NO",
            "-lco", "SPATIAL_INDEX=GIST",
        ]

    mode = "append (data-only)" if data_only else "overwrite (schema-owning)"
    LOG.info("Loading %s -> %s [%s]", src_path.name, table, mode)
    t0 = time.time()
    subprocess.run(cmd, check=True)
    LOG.info("%s loaded in %.1fs", table, time.time() - t0)


def load_basins(libpq_url: str, geojson_path: Path, data_only: bool = False) -> None:
    """Bulk-load the GeoJSON -> aq_basins_raw (geometry column `geom`)."""
    _ogr2ogr_load(
        libpq_url,
        geojson_path,
        table="aq_basins_raw",
        geometry_name="geom",
        data_only=data_only,
    )


def load_gadm(libpq_url: str, gpkg_path: Path, data_only: bool = False) -> None:
    """Bulk-load `gadm36_0.gpkg` -> `gadm36_0`.

    The geometry column is named `the_geom` (not the ogr2ogr default
    `geom`) because `aqueduct.services.gadm_service.GADMService` queries
    `g.the_geom` directly.
    """
    _ogr2ogr_load(
        libpq_url,
        gpkg_path,
        table="gadm36_0",
        geometry_name="the_geom",
        data_only=data_only,
        layer="gadm36_0",
    )


def post_load_basin_indexes(conn) -> None:
    LOG.info("Creating supplemental btree indexes on aq_basins_raw")
    with conn.cursor() as cur:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS aq_basins_raw_pfaf_idx "
            "ON aq_basins_raw (pfaf_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS aq_basins_raw_iso_idx "
            "ON aq_basins_raw (gid_0)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS aq_basins_raw_state_idx "
            "ON aq_basins_raw (name_0, name_1)"
        )
    conn.commit()


def load_sbtn(conn, csv_path: Path, data_only: bool = False) -> None:
    if not csv_path.exists():
        raise SystemExit(f"missing CSV: {csv_path}")
    LOG.info("Loading SBTN -> sbtn_son_v2")
    t0 = time.time()
    with conn.cursor() as cur:
        if data_only:
            cur.execute("TRUNCATE TABLE sbtn_son_v2")
        else:
            cur.execute("DROP TABLE IF EXISTS sbtn_son_v2")
            cur.execute(
                """
                CREATE TABLE sbtn_son_v2 (
                    pfaf_id        BIGINT PRIMARY KEY,
                    sbtn_quant_max DOUBLE PRECISION,
                    sbtn_qual_max  DOUBLE PRECISION
                )
                """
            )
        # CSV columns are: <unnamed index>, SBTN_quant_max, SBTN_qual_max, pfaf_id
        cur.execute(
            """
            CREATE TEMP TABLE _sbtn_stage (
                row_idx        BIGINT,
                sbtn_quant_max DOUBLE PRECISION,
                sbtn_qual_max  DOUBLE PRECISION,
                pfaf_id        BIGINT
            ) ON COMMIT DROP
            """
        )
        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                "COPY _sbtn_stage FROM STDIN WITH (FORMAT csv, HEADER true)",
                f,
            )
        cur.execute(
            """
            INSERT INTO sbtn_son_v2 (pfaf_id, sbtn_quant_max, sbtn_qual_max)
            SELECT pfaf_id, sbtn_quant_max, sbtn_qual_max FROM _sbtn_stage
            ON CONFLICT (pfaf_id) DO NOTHING
            """
        )
    conn.commit()
    LOG.info("sbtn_son_v2 loaded in %.1fs", time.time() - t0)


def load_crops(conn, csv_path: Path, data_only: bool = False) -> None:
    if not csv_path.exists():
        raise SystemExit(f"missing CSV: {csv_path}")
    LOG.info("Loading crop production -> crop_production_pfaf")
    t0 = time.time()
    with conn.cursor() as cur:
        if data_only:
            cur.execute("TRUNCATE TABLE crop_production_pfaf")
        else:
            cur.execute("DROP TABLE IF EXISTS crop_production_pfaf")
            cur.execute(
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
        # CSV columns are: pfaf_id, commodity_code, Basin Production, Irrigation
        cur.execute(
            """
            CREATE TEMP TABLE _crops_stage (
                pfaf_id          BIGINT,
                commodity_code   TEXT,
                basin_production DOUBLE PRECISION,
                irrigation       TEXT
            ) ON COMMIT DROP
            """
        )
        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                "COPY _crops_stage FROM STDIN WITH (FORMAT csv, HEADER true)",
                f,
            )
        cur.execute(
            """
            INSERT INTO crop_production_pfaf
                (pfaf_id, commodity_code, basin_production, irrigation)
            SELECT pfaf_id, commodity_code, basin_production, irrigation
            FROM _crops_stage
            ON CONFLICT (pfaf_id, commodity_code, irrigation) DO NOTHING
            """
        )
        if not data_only:
            cur.execute(
                "CREATE INDEX IF NOT EXISTS crop_prod_lookup_idx "
                "ON crop_production_pfaf (commodity_code, irrigation, pfaf_id)"
            )
    conn.commit()
    LOG.info("crop_production_pfaf loaded in %.1fs", time.time() - t0)


def report_counts(conn) -> None:
    LOG.info("Final row counts:")
    with conn.cursor() as cur:
        for tbl in (
            "aq_basins_raw",
            "sbtn_son_v2",
            "crop_production_pfaf",
            "gadm36_0",
        ):
            cur.execute("SELECT to_regclass(%s)", (f"public.{tbl}",))
            (exists,) = cur.fetchone()
            if not exists:
                LOG.info("  %-25s (not loaded)", tbl)
                continue
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tbl))
            )
            (n,) = cur.fetchone()
            LOG.info("  %-25s %d rows", tbl, n)
        cur.execute("SELECT to_regclass('public.aq_basins_raw')")
        if cur.fetchone()[0]:
            cur.execute("SELECT COUNT(DISTINCT pfaf_id) FROM aq_basins_raw")
            (uniq,) = cur.fetchone()
            LOG.info("  unique pfaf_id in aq_basins_raw: %d", uniq)


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )
    args = parse_args()
    libpq = normalize_pg_url(args.postgres_url)
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        sys.exit(f"data dir not found: {data_dir}")

    if args.only:
        only = set(args.only)
        run_basins = "basins" in only
        run_sbtn = "sbtn" in only
        run_crops = "crops" in only
        run_gadm = "gadm" in only
    else:
        run_basins = not args.skip_basins
        run_sbtn = not args.skip_sbtn
        run_crops = not args.skip_crops
        run_gadm = not args.skip_gadm

    if args.data_only:
        # Schema (tables, indexes, extension) is owned by Alembic
        # migrations; just verify the tables exist before loading.
        conn = psycopg2.connect(**pg_kwargs(libpq))
        try:
            with conn.cursor() as cur:
                missing = []
                for tbl in (
                    "aq_basins_raw",
                    "sbtn_son_v2",
                    "crop_production_pfaf",
                    "gadm36_0",
                ):
                    cur.execute("SELECT to_regclass(%s)", (f"public.{tbl}",))
                    if cur.fetchone()[0] is None:
                        missing.append(tbl)
                if missing:
                    sys.exit(
                        "data-only mode but tables are missing: "
                        f"{', '.join(missing)}. Run 'alembic upgrade head' first."
                    )
        finally:
            conn.close()
    else:
        conn = psycopg2.connect(**pg_kwargs(libpq))
        try:
            ensure_postgis(conn)
        finally:
            conn.close()

    if run_basins:
        load_basins(libpq, data_dir / GEOJSON_FILE, data_only=args.data_only)
    if run_gadm:
        load_gadm(libpq, data_dir / GADM_FILE, data_only=args.data_only)

    conn = psycopg2.connect(**pg_kwargs(libpq))
    try:
        if run_basins and not args.data_only:
            post_load_basin_indexes(conn)
        if run_sbtn:
            load_sbtn(conn, data_dir / SBTN_FILE, data_only=args.data_only)
        if run_crops:
            load_crops(conn, data_dir / CROPS_FILE, data_only=args.data_only)
        report_counts(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
