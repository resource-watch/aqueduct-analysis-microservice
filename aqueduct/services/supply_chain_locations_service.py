"""Supply-chain locations analysis (point/state/country -> per-basin results).

Mirrors the analysis in `AqFoodSupplyChainAnalyzer2026v2.ipynb`. Each input
location is one of three modes (auto-inferred from the fields supplied):

  - "point":   lat/lng + radius + radius_units. We build a buffer (planar
               decimal-degrees by default to match the notebook; opt-in
               geodesic via `buffer_mode="geodesic"`) and intersect it
               against `aq_basins_raw` (cells 7-10).
  - "state":   country + state (+/- iso_code). Selects every basin row
               whose `gid_0`/`name_0`/`name_1` match (cells 11-13).
  - "country": country (+/- iso_code). Selects every basin row whose
               `gid_0`/`name_0` match (cells 14-15).

After collecting hits we dissolve by `(unique_id, pfaf_id, gid_1)` — v2
granularity that keeps separate admin-1 slices within multi-state basins
(notebook `dissolve(by=['UniqueID','pfaf_id','gid_1'])`), enrich with SBTN
(`sbtn_son_v2`) + production (`crop_production_pfaf`), and allocate
`total_volume` proportionally:

    production_sourced_from_basin =
        total_volume * (basin_production / sum(basin_production))

Rows with null or zero `production_sourced_from_basin` are dropped (v2).
Sentinel `pfaf_id = -9999` and `gid_1 = '-9999'` rows are excluded.
`irrigation = "Unknown"` is normalized to `"All"` before the prod join.

The whole pipeline is one PostGIS query — no GeoPandas at request time.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from aqueduct.services.supply_chain_data.commodities import resolve_commodity
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

LOG = logging.getLogger(__name__)


# Radius unit -> conversion factor to KILOMETERS, matching notebook cell 3
# (`clean_buffer`).
_KM_PER_UNIT = {
    "miles": 1.609,
    "mile": 1.609,
    "km": 1.0,
    "kilometer": 1.0,
    "kilometers": 1.0,
    "m": 0.001,
    "met": 0.001,
    "meter": 0.001,
    "meters": 0.001,
}

# Same set, lower-cased, used by the validator.
ALLOWED_RADIUS_UNITS = sorted(_KM_PER_UNIT.keys())

# Irrigation values present in `crop_production_pfaf.irrigation`.
ALLOWED_IRRIGATION = ["All", "Irrigated", "Rainfed"]

# Map any-case irrigation input (and the notebook "Unknown" placeholder) to the
# canonical values stored in `crop_production_pfaf.irrigation`. The frontend
# template sends lower-case values ("rainfed", "irrigated"), so normalization
# must be case-insensitive. Values not present here are returned unchanged so
# the request validator's `allowed` check can still reject genuine garbage.
_IRRIGATION_ALIASES = {
    "all": "All",
    "irrigated": "Irrigated",
    "rainfed": "Rainfed",
    "unknown": "All",
}

# Buffer modes
BUFFER_PLANAR = "planar"
BUFFER_GEODESIC = "geodesic"
ALLOWED_BUFFER_MODES = [BUFFER_PLANAR, BUFFER_GEODESIC]


def radius_to_km(radius: float, units: str) -> float:
    """Convert (radius, units) -> kilometers."""
    if radius is None or radius <= 0:
        raise ValueError("radius must be > 0")
    factor = _KM_PER_UNIT.get((units or "").lower())
    if factor is None:
        raise ValueError(f"unsupported radius units: {units!r}")
    return float(radius) * factor


def radius_to_degrees(radius: float, units: str) -> float:
    """Convert (radius, units) -> decimal degrees, matching the notebook.

    Notebook semantics: km value / 111. Not metrically accurate at high
    latitudes but kept identical so results match cell-for-cell.
    """
    return radius_to_km(radius, units) / 111.0


def infer_select_by(loc: dict) -> str | None:
    """Infer location mode (point/state/country) from the fields present.

    Mirrors notebook cell 3 `find_selection_type`, but also lets the
    caller pin the mode explicitly with a `select_by` field.
    """
    explicit = loc.get("select_by")
    if explicit:
        return str(explicit).lower()
    if loc.get("lat") is not None and loc.get("lng") is not None:
        return "point"
    if loc.get("state"):
        return "state"
    if loc.get("country") or loc.get("iso_code"):
        return "country"
    return None


def normalize_irrigation(value):
    """Canonicalize an irrigation value to match `crop_production_pfaf`.

    Case-insensitive ("rainfed" -> "Rainfed") and maps the notebook "Unknown"
    placeholder to "All". Unrecognized values are returned unchanged (as a
    string) so the request validator can reject them via its `allowed` list.
    """
    if value is None:
        return value
    return _IRRIGATION_ALIASES.get(str(value).strip().lower(), str(value))


# Buffer expressions used inside the CTE. Selected by `buffer_mode`.
_BUFFER_PLANAR_EXPR = (
    "ST_Buffer(ST_SetSRID(ST_MakePoint(lng, lat), 4326), radius_deg)"
)
_BUFFER_GEODESIC_EXPR = (
    "ST_Buffer(ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography, "
    "radius_m)::geometry"
)

# Single CTE query that handles all three location modes. The inputs CTE
# carries every field; rows for non-applicable columns are NULL. Three
# parallel `*_hits` CTEs each filter to their mode and emit a uniform
# row shape that gets UNION'd into `hits`.
_ANALYSIS_SQL_TEMPLATE = """
WITH inputs(
        unique_id, select_by, lat, lng, radius_deg, radius_m,
        iso_code, country, state,
        commodity, irrigation, total_volume
    ) AS (
    VALUES %s
),
point_buffers AS (
    SELECT
        unique_id,
        commodity, irrigation, total_volume,
        {buffer_expr} AS geom
    FROM inputs
    WHERE select_by = 'point'
),
point_hits AS (
    SELECT
        b.unique_id,
        ar.pfaf_id,
        ar.gid_1,
        b.commodity, b.irrigation, b.total_volume,
        MAX(ar.gid_0)     AS iso_code,
        MAX(ar.name_0)    AS country,
        MAX(ar.name_1)    AS state,
        MAX(ar.bws_raw)   AS bws_raw,
        MAX(ar.bws_score) AS bws_score,
        MAX(ar.bws_cat)   AS bws_cat,
        MAX(ar.bws_label) AS bws_label{geom_agg}
    FROM point_buffers b
    JOIN aq_basins_raw ar ON ST_Intersects(ar.geom, b.geom)
    WHERE ar.pfaf_id <> -9999
      AND ar.gid_1 IS NOT NULL
      AND ar.gid_1::text <> '-9999'
    GROUP BY b.unique_id, ar.pfaf_id, ar.gid_1,
             b.commodity, b.irrigation, b.total_volume
),
admin_hits AS (
    -- `country` is matched permissively against either `name_0` (full
    -- country name like "Kenya") or `gid_0` (ISO3 like "KEN"), so callers
    -- can send whichever they have. `iso_code` always goes against
    -- `gid_0` (validator guarantees it's a 2-3 letter code).
    --
    -- Dissolve granularity depends on the selection mode: `state` searches
    -- keep the admin-1 (`gid_1`) split, but `country` searches dissolve each
    -- basin (`pfaf_id`) into a single feature spanning the whole country.
    -- Keeping the `gid_1` split for country mode would both fragment the
    -- geometry and double-count `basin_production` (which is keyed by
    -- `pfaf_id`), inflating `summed_production` and skewing the allocation.
    SELECT
        i.unique_id,
        ar.pfaf_id,
        CASE WHEN i.select_by = 'country' THEN NULL ELSE ar.gid_1 END AS gid_1,
        i.commodity, i.irrigation, i.total_volume,
        MAX(ar.gid_0)     AS iso_code,
        MAX(ar.name_0)    AS country,
        MAX(CASE WHEN i.select_by = 'country' THEN NULL ELSE ar.name_1 END) AS state,
        MAX(ar.bws_raw)   AS bws_raw,
        MAX(ar.bws_score) AS bws_score,
        MAX(ar.bws_cat)   AS bws_cat,
        MAX(ar.bws_label) AS bws_label{geom_agg}
    FROM inputs i
    JOIN aq_basins_raw ar ON
        ar.pfaf_id <> -9999
        AND ar.gid_1 IS NOT NULL
        AND ar.gid_1::text <> '-9999'
        AND (i.iso_code IS NULL OR LOWER(ar.gid_0) = LOWER(i.iso_code))
        AND (
            i.country IS NULL
            OR LOWER(ar.name_0) = LOWER(i.country)
            OR LOWER(ar.gid_0)  = LOWER(i.country)
        )
        AND (
            i.select_by = 'country'
            OR LOWER(ar.name_1) = LOWER(i.state)
        )
    WHERE i.select_by IN ('state', 'country')
    GROUP BY i.unique_id, ar.pfaf_id,
             CASE WHEN i.select_by = 'country' THEN NULL ELSE ar.gid_1 END,
             i.commodity, i.irrigation, i.total_volume, i.select_by
),
hits AS (
    SELECT * FROM point_hits
    UNION ALL
    SELECT * FROM admin_hits
),
enriched AS (
    SELECT
        h.*,
        p.basin_production,
        s.sbtn_quant_max,
        s.sbtn_qual_max
    FROM hits h
    LEFT JOIN crop_production_pfaf p
           ON p.pfaf_id        = h.pfaf_id
          AND p.commodity     = h.commodity
          AND p.irrigation     = h.irrigation
    LEFT JOIN sbtn_son_v2 s ON s.pfaf_id = h.pfaf_id
),
sums AS (
    SELECT unique_id,
           SUM(basin_production) AS summed_production
    FROM enriched
    GROUP BY unique_id
),
allocated AS (
    SELECT e.unique_id,
           e.pfaf_id,
           e.gid_1,
           e.iso_code,
           e.country,
           e.state,
           e.commodity,
           e.irrigation,
           e.total_volume,
           e.bws_raw,
           e.bws_score,
           e.bws_cat,
           e.bws_label,
           e.sbtn_quant_max,
           e.sbtn_qual_max,
           e.basin_production,
           s.summed_production,
           CASE
               WHEN s.summed_production IS NOT NULL
                    AND s.summed_production > 0
                    AND e.total_volume IS NOT NULL
                    AND e.basin_production IS NOT NULL
               THEN e.total_volume * (e.basin_production / s.summed_production)
               ELSE NULL
           END AS production_sourced_from_basin{geom_final}
    FROM enriched e
    LEFT JOIN sums s USING (unique_id)
)
SELECT *
FROM allocated
WHERE production_sourced_from_basin IS NOT NULL
  AND production_sourced_from_basin <> 0
ORDER BY unique_id, pfaf_id, gid_1
"""

_VALUES_TEMPLATE = (
    "(%s::text, %s::text, "                       # unique_id, select_by
    "%s::float8, %s::float8, %s::float8, %s::float8, "  # lat, lng, radius_deg, radius_m
    "%s::text, %s::text, %s::text, "              # iso_code, country, state
    "%s::text, %s::text, %s::float8)"             # commodity, irrigation, vol
)


class SupplyChainLocationsService:
    """Service object for the supply-chain analysis (point/state/country)."""

    def __init__(self, postgres_url: str | None = None) -> None:
        self._dsn = self._normalize_url(
            postgres_url or os.environ.get("POSTGRES_URL")
        )
        if not self._dsn:
            raise RuntimeError("POSTGRES_URL is not configured")

    @staticmethod
    def _normalize_url(url: str | None) -> str | None:
        if not url:
            return None
        if url.startswith("postgresql+psycopg2://"):
            return "postgresql://" + url[len("postgresql+psycopg2://") :]
        return url

    def analyze(
        self,
        locations: Iterable[dict],
        buffer_mode: str = BUFFER_PLANAR,
        include_geometry: bool = False,
        simplify: float | None = None,
    ) -> dict[str, Any]:
        """Run the full analysis for a batch of input locations.

        Parameters
        ----------
        locations:
            Already-validated location dicts (see validator). Each must
            carry enough fields to infer a select_by mode.
        buffer_mode:
            "planar" (default, matches notebook cells 7-10) buffers in
            decimal degrees. "geodesic" buffers on the spheroid in
            meters — more accurate at high latitudes but diverges from
            notebook output.
        include_geometry:
            When True, the response gains a `geojson` FeatureCollection
            whose features carry the dissolved basin polygon (one per
            `(unique_id, pfaf_id, gid_1)`) and the full analysis row as
            `properties`. Off by default to keep payloads small.
        simplify:
            Optional Douglas-Peucker tolerance in degrees applied to the
            returned geometry (ST_SimplifyPreserveTopology). Only used
            when `include_geometry` is True. Smaller = more detail.
        """
        if buffer_mode not in ALLOWED_BUFFER_MODES:
            raise ValueError(
                f"buffer_mode must be one of {ALLOWED_BUFFER_MODES}, "
                f"got {buffer_mode!r}"
            )

        prepared, prep_errors = self._prepare_inputs(list(locations))

        if not prepared:
            response: dict[str, Any] = {"results": [], "errors": prep_errors}
            if include_geometry:
                response["geojson"] = {
                    "type": "FeatureCollection",
                    "features": [],
                }
            return response

        buffer_expr = (
            _BUFFER_GEODESIC_EXPR
            if buffer_mode == BUFFER_GEODESIC
            else _BUFFER_PLANAR_EXPR
        )

        if include_geometry:
            # Dissolve admin slices into one polygon per (pfaf_id, gid_1),
            # then emit as GeoJSON at 6-decimal precision (~0.11 m),
            # optionally simplified to shrink the payload.
            geom_agg = ",\n        ST_Union(ar.geom) AS geom"
            if simplify is not None and simplify > 0:
                geom_src = (
                    "COALESCE("
                    f"ST_SimplifyPreserveTopology(e.geom, {float(simplify)}), "
                    "e.geom)"
                )
            else:
                geom_src = "e.geom"
            geom_final = f",\n       ST_AsGeoJSON({geom_src}, 6) AS geometry"
        else:
            geom_agg = ""
            geom_final = ""

        sql = _ANALYSIS_SQL_TEMPLATE.format(
            buffer_expr=buffer_expr,
            geom_agg=geom_agg,
            geom_final=geom_final,
        )

        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                LOG.debug(
                    "[supply_chain_locations] running analysis "
                    "(mode=%s, geometry=%s) for %d inputs",
                    buffer_mode,
                    include_geometry,
                    len(prepared),
                )
                execute_values(
                    cur,
                    sql,
                    [row["values"] for row in prepared],
                    template=_VALUES_TEMPLATE,
                )
                rows = cur.fetchall()

        results = []
        features = []
        for row in rows:
            geom_json = row.pop("geometry", None) if include_geometry else None
            formatted = self._format_row(row)
            results.append(formatted)
            if include_geometry:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": json.loads(geom_json) if geom_json else None,
                        "properties": formatted,
                    }
                )

        seen = {r["unique_id"] for r in results}
        miss_errors = [
            {
                "unique_id": p["values"][0],
                "reason": (
                    "buffer did not intersect any basins"
                    if p["values"][1] == "point"
                    else "no basins matched the supplied admin name"
                ),
            }
            for p in prepared
            if p["values"][0] not in seen
        ]

        response = {"results": results, "errors": prep_errors + miss_errors}
        if include_geometry:
            response["geojson"] = {
                "type": "FeatureCollection",
                "features": features,
            }
        return response

    @staticmethod
    def _prepare_inputs(
        locations: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Normalize each location into a tuple matching `_VALUES_TEMPLATE`.

        Returns (prepared, errors). Errors are emitted for inputs that
        fail mode-specific shape requirements (e.g. point without radius).
        """
        prepared: list[dict] = []
        errors: list[dict] = []
        for idx, loc in enumerate(locations):
            unique_id = str(loc.get("unique_id") or f"loc-{idx}")
            select_by = infer_select_by(loc)
            if select_by not in ("point", "state", "country"):
                errors.append(
                    {
                        "unique_id": unique_id,
                        "reason": (
                            "could not infer select_by; supply lat+lng or "
                            "country (+state)"
                        ),
                    }
                )
                continue

            commodity = resolve_commodity(loc)
            if not commodity:
                errors.append(
                    {
                        "unique_id": unique_id,
                        "reason": "unknown or missing commodity",
                    }
                )
                continue

            lat = lng = radius_deg = radius_m = None
            if select_by == "point":
                if (
                    loc.get("lat") is None
                    or loc.get("lng") is None
                    or loc.get("radius") is None
                    or loc.get("radius_units") is None
                ):
                    errors.append(
                        {
                            "unique_id": unique_id,
                            "reason": (
                                "point mode requires lat, lng, radius, "
                                "radius_units"
                            ),
                        }
                    )
                    continue
                try:
                    km = radius_to_km(loc["radius"], loc["radius_units"])
                except ValueError as exc:
                    errors.append({"unique_id": unique_id, "reason": str(exc)})
                    continue
                lat = float(loc["lat"])
                lng = float(loc["lng"])
                radius_deg = km / 111.0
                radius_m = km * 1000.0

            if select_by == "state" and not loc.get("state"):
                errors.append(
                    {
                        "unique_id": unique_id,
                        "reason": "state mode requires the 'state' field",
                    }
                )
                continue
            if select_by in ("state", "country") and not (
                loc.get("country") or loc.get("iso_code")
            ):
                errors.append(
                    {
                        "unique_id": unique_id,
                        "reason": (
                            "admin mode requires 'country' or 'iso_code'"
                        ),
                    }
                )
                continue

            prepared.append(
                {
                    "values": (
                        unique_id,
                        select_by,
                        lat,
                        lng,
                        radius_deg,
                        radius_m,
                        loc.get("iso_code"),
                        loc.get("country"),
                        loc.get("state"),
                        commodity,
                        normalize_irrigation(str(loc["irrigation"])),
                        (
                            float(loc["total_volume"])
                            if loc.get("total_volume") is not None
                            else None
                        ),
                    )
                }
            )
        return prepared, errors

    @staticmethod
    def _format_row(row: dict) -> dict:
        out = dict(row)
        for k in (
            "bws_raw",
            "bws_score",
            "bws_cat",
            "sbtn_quant_max",
            "sbtn_qual_max",
            "basin_production",
            "summed_production",
            "production_sourced_from_basin",
            "total_volume",
        ):
            if out.get(k) is not None:
                out[k] = float(out[k])
        if out.get("pfaf_id") is not None:
            out["pfaf_id"] = int(out["pfaf_id"])
        return out
