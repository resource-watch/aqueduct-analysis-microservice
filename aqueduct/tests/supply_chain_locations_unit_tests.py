"""Pure-unit tests for the supply-chain locations service.

These tests do not touch the database; they exercise the conversion
helpers, the input-normalizer, and the request validator. Database-
dependent behavior is covered separately in the route tests, where
`psycopg2.connect` is mocked.
"""

import math

import pytest

from aqueduct.services.supply_chain_locations_service import (
    ALLOWED_BUFFER_MODES,
    ALLOWED_IRRIGATION,
    ALLOWED_RADIUS_UNITS,
    SupplyChainLocationsService,
    infer_select_by,
    normalize_irrigation,
    radius_to_degrees,
    radius_to_km,
)


# ---------------------------------------------------------------------------
# radius_to_km / radius_to_degrees
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "radius,units,expected_km",
    [
        (1, "km", 1.0),
        (1, "kilometer", 1.0),
        (1, "kilometers", 1.0),
        (1000, "m", 1.0),
        (1000, "meter", 1.0),
        (1000, "meters", 1.0),
        (1000, "met", 1.0),
        (1, "mile", 1.609),
        (1, "miles", 1.609),
        (1, "KM", 1.0),  # case-insensitive
    ],
)
def test_radius_to_km(radius, units, expected_km):
    assert math.isclose(radius_to_km(radius, units), expected_km, rel_tol=1e-9)


def test_radius_to_degrees_matches_notebook_formula():
    # Notebook cell 3: km / 111
    assert math.isclose(radius_to_degrees(50, "km"), 50.0 / 111.0)
    assert math.isclose(
        radius_to_degrees(10, "miles"), (10 * 1.609) / 111.0
    )
    assert math.isclose(
        radius_to_degrees(5000, "meters"), (5.0) / 111.0
    )


def test_radius_to_km_rejects_zero_or_negative():
    with pytest.raises(ValueError, match="radius must be > 0"):
        radius_to_km(0, "km")
    with pytest.raises(ValueError, match="radius must be > 0"):
        radius_to_km(-3, "km")


def test_radius_to_km_rejects_unknown_units():
    with pytest.raises(ValueError, match="unsupported radius units"):
        radius_to_km(10, "furlongs")


def test_allowed_constants_are_sorted_and_complete():
    assert ALLOWED_RADIUS_UNITS == sorted(ALLOWED_RADIUS_UNITS)
    assert "km" in ALLOWED_RADIUS_UNITS
    assert "miles" in ALLOWED_RADIUS_UNITS
    assert "meters" in ALLOWED_RADIUS_UNITS
    assert ALLOWED_IRRIGATION == ["All", "Irrigated", "Rainfed"]
    assert set(ALLOWED_BUFFER_MODES) == {"planar", "geodesic"}


# ---------------------------------------------------------------------------
# infer_select_by
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "loc,expected",
    [
        ({"lat": 0, "lng": 0}, "point"),
        ({"lat": 0, "lng": 0, "country": "Brazil"}, "point"),  # lat wins
        ({"country": "Brazil", "state": "São Paulo"}, "state"),
        ({"country": "Brazil"}, "country"),
        ({"iso_code": "BRA"}, "country"),
        ({"select_by": "country", "iso_code": "USA"}, "country"),
        ({"select_by": "STATE", "country": "Foo", "state": "Bar"}, "state"),
        ({"commodity": "Soybean"}, None),
    ],
)
def test_infer_select_by(loc, expected):
    assert infer_select_by(loc) == expected


def test_normalize_irrigation_maps_unknown_to_all():
    assert normalize_irrigation("Unknown") == "All"
    assert normalize_irrigation("All") == "All"


@pytest.mark.parametrize(
    "value,expected",
    [
        # Frontend template sends lower-case irrigation values.
        ("rainfed", "Rainfed"),
        ("irrigated", "Irrigated"),
        ("all", "All"),
        ("RAINFED", "Rainfed"),
        (" Irrigated ", "Irrigated"),
        ("unknown", "All"),
        # Canonical values are preserved.
        ("Rainfed", "Rainfed"),
        ("Irrigated", "Irrigated"),
    ],
)
def test_normalize_irrigation_is_case_insensitive(value, expected):
    assert normalize_irrigation(value) == expected


def test_normalize_irrigation_passes_through_unknown_values():
    # Garbage stays unchanged so the request validator's `allowed` check rejects it.
    assert normalize_irrigation("Sometimes") == "Sometimes"


def test_prepare_inputs_normalizes_lowercase_irrigation():
    locs = [
        {
            "country": "Brazil",
            "commodity": "Maize",
            "irrigation": "rainfed",
        }
    ]
    prepared, errors = SupplyChainLocationsService._prepare_inputs(locs)
    assert errors == []
    assert prepared[0]["values"][10] == "Rainfed"


def test_prepare_inputs_normalizes_unknown_irrigation():
    locs = [
        {
            "country": "Brazil",
            "commodity": "Soybean",
            "irrigation": "Unknown",
        }
    ]
    prepared, errors = SupplyChainLocationsService._prepare_inputs(locs)
    assert errors == []
    assert prepared[0]["values"][10] == "All"


# ---------------------------------------------------------------------------
# SupplyChainLocationsService._prepare_inputs
# ---------------------------------------------------------------------------


def _values(prepared):
    """Helper: list of the `values` tuples in submission order."""
    return [p["values"] for p in prepared]


def test_prepare_inputs_point_happy_path():
    locs = [
        {
            "unique_id": "p1",
            "lat": -23.55,
            "lng": -46.63,
            "radius": 50,
            "radius_units": "km",
            "commodity_code": "soyb",  # legacy code still accepted
            "irrigation": "All",
            "total_volume": 12000,
        }
    ]
    prepared, errors = SupplyChainLocationsService._prepare_inputs(locs)
    assert errors == []
    assert len(prepared) == 1
    (
        unique_id,
        select_by,
        lat,
        lng,
        radius_deg,
        radius_m,
        iso,
        country,
        state,
        commodity,
        irrig,
        vol,
    ) = prepared[0]["values"]
    assert unique_id == "p1"
    assert select_by == "point"
    assert lat == -23.55
    assert lng == -46.63
    assert math.isclose(radius_deg, 50.0 / 111.0)
    assert math.isclose(radius_m, 50_000.0)
    assert (iso, country, state) == (None, None, None)
    assert commodity == "Soybean"
    assert irrig == "All"
    assert vol == 12000.0


def test_prepare_inputs_state_and_country_modes():
    locs = [
        {
            "unique_id": "s1",
            "country": "Brazil",
            "state": "São Paulo",
            "commodity": "Soybean",
            "irrigation": "All",
        },
        {
            "iso_code": "USA",
            "commodity": "Maize",
            "irrigation": "Rainfed",
            "total_volume": 5000,
        },
    ]
    prepared, errors = SupplyChainLocationsService._prepare_inputs(locs)
    assert errors == []
    [a, b] = _values(prepared)
    assert a[0] == "s1" and a[1] == "state"
    assert (a[6], a[7], a[8]) == (None, "Brazil", "São Paulo")
    assert b[0] == "loc-1"  # unique_id auto-assigned
    assert b[1] == "country"
    assert (b[6], b[7], b[8]) == ("USA", None, None)


def test_prepare_inputs_collects_per_location_errors():
    locs = [
        {"commodity": "Soybean", "irrigation": "All"},  # no location info
        {
            "lat": 0,
            "lng": 0,
            "radius": 0,  # invalid
            "radius_units": "km",
            "commodity": "Soybean",
            "irrigation": "All",
        },
        {
            "lat": 0,
            "lng": 0,
            "radius": 1,
            "radius_units": "furlongs",  # invalid
            "commodity": "Soybean",
            "irrigation": "All",
        },
        {  # state mode but no country and no iso_code
            "state": "São Paulo",
            "commodity": "Soybean",
            "irrigation": "All",
        },
    ]
    prepared, errors = SupplyChainLocationsService._prepare_inputs(locs)
    assert prepared == []
    reasons = [e["reason"] for e in errors]
    assert any("could not infer" in r for r in reasons)
    assert any("radius must be > 0" in r for r in reasons)
    assert any("unsupported radius units" in r for r in reasons)
    assert any("'country' or 'iso_code'" in r for r in reasons)


def test_prepare_inputs_accepts_legacy_commodity_code():
    locs = [
        {
            "country": "Brazil",
            "commodity_code": "BANA",
            "irrigation": "All",
        }
    ]
    prepared, errors = SupplyChainLocationsService._prepare_inputs(locs)
    assert errors == []
    assert prepared[0]["values"][9] == "Banana"


def test_prepare_inputs_explicit_select_by_pins_mode():
    """Explicit select_by='country' should ignore stray lat/lng."""
    locs = [
        {
            "select_by": "country",
            "lat": 0,
            "lng": 0,
            "country": "Brazil",
            "commodity": "Soybean",
            "irrigation": "All",
        }
    ]
    prepared, errors = SupplyChainLocationsService._prepare_inputs(locs)
    assert errors == []
    assert prepared[0]["values"][1] == "country"
