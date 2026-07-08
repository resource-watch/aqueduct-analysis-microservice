"""Route tests for POST /food-supply-chain/locations.

These tests exercise the full request->response path through Flask but
mock `psycopg2.connect` inside the service so they don't need a live
PostGIS instance. They verify the validator wiring, the request body
shape, the per-location error reporting, and the response formatting.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


ENDPOINT = "/api/v1/aqueduct/analysis/food-supply-chain/locations"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _patch_psycopg2(rows):
    """Build a context manager that patches `psycopg2.connect` to return
    `rows` for the next `cur.fetchall()` call.
    """

    cursor = MagicMock()
    cursor.fetchall.return_value = rows

    cursor_ctx = MagicMock()
    cursor_ctx.__enter__.return_value = cursor
    cursor_ctx.__exit__.return_value = False

    conn = MagicMock()
    conn.cursor.return_value = cursor_ctx

    conn_ctx = MagicMock()
    conn_ctx.__enter__.return_value = conn
    conn_ctx.__exit__.return_value = False

    return patch(
        "aqueduct.services.supply_chain_locations_service.psycopg2.connect",
        return_value=conn_ctx,
    ), patch(
        # `execute_values` is imported by name into the service module;
        # silence it so it doesn't complain about the MagicMock cursor.
        "aqueduct.services.supply_chain_locations_service.execute_values",
        return_value=None,
    )


def _basin_row(unique_id, pfaf_id, basin_production, total_volume,
               summed=None, sourced=None, country="Brazil", gid_1="BRA.25_1"):
    """Convenience: produce a fake DB row matching what the SQL returns."""
    return {
        "unique_id": unique_id,
        "pfaf_id": pfaf_id,
        "gid_1": gid_1,
        "iso_code": "BRA",
        "country": country,
        "state": "São Paulo",
        "commodity": "Soybean",
        "irrigation": "All",
        "total_volume": total_volume,
        "bws_raw": 0.1,
        "bws_score": 1.0,
        "bws_cat": 1.0,
        "bws_label": "Low (<10%)",
        "sbtn_quant_max": 5.0,
        "sbtn_qual_max": 3.0,
        "basin_production": basin_production,
        "summed_production": summed,
        "production_sourced_from_basin": sourced,
    }


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------


def test_endpoint_happy_path_returns_results(client):
    rows = [
        _basin_row("p1", 111111, 100.0, 12000, summed=400.0, sourced=3000.0),
        _basin_row("p1", 222222, 300.0, 12000, summed=400.0, sourced=9000.0),
    ]
    pg_patch, ev_patch = _patch_psycopg2(rows)
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "unique_id": "p1",
                            "lat": -23.55,
                            "lng": -46.63,
                            "radius": 50,
                            "radius_units": "km",
                            "commodity": "Soybean",
                            "irrigation": "All",
                            "total_volume": 12000,
                        }
                    ]
                }
            ),
            content_type="application/json",
        )

    assert resp.status_code == 200
    body = json.loads(resp.data)
    assert body["errors"] == []
    assert len(body["results"]) == 2
    assert {r["pfaf_id"] for r in body["results"]} == {111111, 222222}
    # numeric values get coerced to floats / ints
    assert all(isinstance(r["pfaf_id"], int) for r in body["results"])
    assert all(
        isinstance(r["production_sourced_from_basin"], float)
        for r in body["results"]
    )


# ---------------------------------------------------------------------------
# validator failures (no DB call needed, but we patch defensively)
# ---------------------------------------------------------------------------


def test_endpoint_rejects_empty_locations(client):
    pg_patch, ev_patch = _patch_psycopg2([])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps({"locations": []}),
            content_type="application/json",
        )
    assert resp.status_code == 400
    body = json.loads(resp.data)
    assert "non-empty 'locations'" in body["errors"][0]["detail"]


def test_endpoint_rejects_invalid_radius_units(client):
    pg_patch, ev_patch = _patch_psycopg2([])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "lat": 0,
                            "lng": 0,
                            "radius": 1,
                            "radius_units": "furlongs",
                            "commodity": "Soybean",
                            "irrigation": "All",
                        }
                    ]
                }
            ),
            content_type="application/json",
        )
    assert resp.status_code == 400
    detail = json.loads(resp.data)["errors"][0]["detail"]
    assert "locations" in detail
    assert "radius_units" in str(detail["locations"]["0"])


def test_endpoint_rejects_invalid_irrigation(client):
    pg_patch, ev_patch = _patch_psycopg2([])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "country": "Brazil",
                            "commodity": "Soybean",
                            "irrigation": "Sometimes",
                        }
                    ]
                }
            ),
            content_type="application/json",
        )
    assert resp.status_code == 400


def test_endpoint_rejects_unknown_buffer_query(client):
    pg_patch, ev_patch = _patch_psycopg2([])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT + "?buffer=ohno",
            data=json.dumps(
                {
                    "locations": [
                        {
                            "lat": 0,
                            "lng": 0,
                            "radius": 1,
                            "radius_units": "km",
                            "commodity": "Soybean",
                            "irrigation": "All",
                        }
                    ]
                }
            ),
            content_type="application/json",
        )
    assert resp.status_code == 400
    body = json.loads(resp.data)
    assert "planar" in body["errors"][0]["detail"]


def test_endpoint_too_many_locations(client):
    pg_patch, ev_patch = _patch_psycopg2([])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "lat": 0,
                            "lng": 0,
                            "radius": 1,
                            "radius_units": "km",
                            "commodity": "Soybean",
                            "irrigation": "All",
                        }
                    ]
                    * 501
                }
            ),
            content_type="application/json",
        )
    assert resp.status_code == 413


# ---------------------------------------------------------------------------
# per-location error reporting (DB returns nothing)
# ---------------------------------------------------------------------------


def test_endpoint_no_intersecting_basins_reported_in_errors(client):
    pg_patch, ev_patch = _patch_psycopg2([])  # SQL finds no rows
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "unique_id": "ocean",
                            "lat": 0,
                            "lng": -150,
                            "radius": 50,
                            "radius_units": "km",
                            "commodity": "Soybean",
                            "irrigation": "All",
                            "total_volume": 1000,
                        }
                    ]
                }
            ),
            content_type="application/json",
        )
    assert resp.status_code == 200
    body = json.loads(resp.data)
    assert body["results"] == []
    assert len(body["errors"]) == 1
    assert body["errors"][0]["unique_id"] == "ocean"
    assert "did not intersect" in body["errors"][0]["reason"]


def test_endpoint_country_field_accepts_iso_code_alongside_iso_code(client):
    """Regression: callers sometimes send `country: "KEN"` and
    `iso_code: "KEN"` together. The DB has `name_0='Kenya'`, so a
    strict `name_0 = country` match would silently zero out the row.
    Service-level SQL must permit `country` to match either name_0
    OR gid_0 (verified via response shape coming back non-empty).
    """
    rows = [
        _basin_row("ken", 123456, 50.0, None, country="Kenya")
    ]
    pg_patch, ev_patch = _patch_psycopg2(rows)
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "unique_id": "ken",
                            "iso_code": "KEN",
                            "country": "KEN",  # caller sent ISO instead of name
                            "state": "Nairobi",
                            "commodity": "Maize",
                            "irrigation": "Rainfed",
                        }
                    ]
                }
            ),
            content_type="application/json",
        )
    assert resp.status_code == 200
    body = json.loads(resp.data)
    assert body["errors"] == []
    assert len(body["results"]) == 1
    assert body["results"][0]["unique_id"] == "ken"


def test_endpoint_admin_no_match_reports_admin_reason(client):
    pg_patch, ev_patch = _patch_psycopg2([])  # SQL finds no rows
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "unique_id": "atlantis",
                            "country": "Atlantis",
                            "commodity": "Soybean",
                            "irrigation": "All",
                        }
                    ]
                }
            ),
            content_type="application/json",
        )
    body = json.loads(resp.data)
    assert resp.status_code == 200
    assert body["results"] == []
    assert body["errors"][0]["unique_id"] == "atlantis"
    assert "admin" in body["errors"][0]["reason"]


# ---------------------------------------------------------------------------
# geometry output (?geometry=true)
# ---------------------------------------------------------------------------


def _point_body(unique_id="p1"):
    return json.dumps(
        {
            "locations": [
                {
                    "unique_id": unique_id,
                    "lat": -23.55,
                    "lng": -46.63,
                    "radius": 50,
                    "radius_units": "km",
                    "commodity": "Soybean",
                    "irrigation": "All",
                    "total_volume": 12000,
                }
            ]
        }
    )


def test_endpoint_no_geojson_key_by_default(client):
    rows = [_basin_row("p1", 111111, 100.0, 12000, summed=100.0, sourced=12000.0)]
    pg_patch, ev_patch = _patch_psycopg2(rows)
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT, data=_point_body(), content_type="application/json"
        )
    body = json.loads(resp.data)
    assert resp.status_code == 200
    assert "geojson" not in body


def test_endpoint_geometry_true_returns_feature_collection(client):
    # Rows include a `geometry` key (GeoJSON string) like ST_AsGeoJSON emits.
    row_a = _basin_row("p1", 111111, 100.0, 12000, summed=400.0, sourced=3000.0)
    row_a["geometry"] = json.dumps(
        {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
    )
    row_b = _basin_row("p1", 222222, 300.0, 12000, summed=400.0, sourced=9000.0)
    row_b["geometry"] = json.dumps(
        {"type": "Polygon", "coordinates": [[[2, 2], [3, 2], [3, 3], [2, 2]]]}
    )
    pg_patch, ev_patch = _patch_psycopg2([row_a, row_b])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT + "?geometry=true",
            data=_point_body(),
            content_type="application/json",
        )
    assert resp.status_code == 200
    body = json.loads(resp.data)

    # tabular results stay clean (no geometry leaked into the rows)
    assert len(body["results"]) == 2
    assert all("geometry" not in r for r in body["results"])

    fc = body["geojson"]
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 2

    feat = fc["features"][0]
    assert feat["type"] == "Feature"
    assert feat["geometry"]["type"] == "Polygon"
    # properties carry the full analysis row, including pfaf_id
    assert feat["properties"]["pfaf_id"] == 111111
    assert feat["properties"]["unique_id"] == "p1"
    assert "production_sourced_from_basin" in feat["properties"]


def test_endpoint_geometry_null_geometry_becomes_none(client):
    # A row whose geometry came back NULL should yield feature.geometry = None.
    row = _basin_row("p1", 111111, 100.0, 12000, summed=100.0, sourced=12000.0)
    row["geometry"] = None
    pg_patch, ev_patch = _patch_psycopg2([row])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT + "?geometry=true",
            data=_point_body(),
            content_type="application/json",
        )
    body = json.loads(resp.data)
    assert resp.status_code == 200
    assert body["geojson"]["features"][0]["geometry"] is None


def test_endpoint_rejects_non_numeric_simplify(client):
    pg_patch, ev_patch = _patch_psycopg2([])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT + "?geometry=true&simplify=abc",
            data=_point_body(),
            content_type="application/json",
        )
    assert resp.status_code == 400
    assert "simplify" in json.loads(resp.data)["errors"][0]["detail"]
