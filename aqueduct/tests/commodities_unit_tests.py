"""Unit tests for commodity name/code normalization."""

import json

import pytest

from aqueduct.services.supply_chain_data.commodities import (
    ALLOWED_COMMODITIES,
    FRONTEND_COMMODITY_NAMES,
    normalize_commodity,
    resolve_commodity,
)
from aqueduct.tests.supply_chain_locations_route_tests import ENDPOINT, _patch_psycopg2


@pytest.mark.parametrize("commodity", FRONTEND_COMMODITY_NAMES)
def test_normalize_frontend_commodity_names(commodity):
    assert normalize_commodity(commodity) == commodity


@pytest.mark.parametrize("commodity", FRONTEND_COMMODITY_NAMES)
def test_frontend_commodity_names_are_allowed(commodity):
    assert commodity in ALLOWED_COMMODITIES


@pytest.mark.parametrize(
    "value,expected",
    [
        ("Rubber", "Rubber"),
        ("rubber", "Rubber"),
        ("RUBB", "Rubber"),
        ("Citrus", "Citrus"),
        ("CITR", "Citrus"),
        ("Pigeon pea", "Pigeon pea"),
        ("pigeonpea", "Pigeon pea"),
        ("Sesame seed", "Sesame seed"),
        ("Other fibres", "Other fibres"),
        ("Other tropical fruit", "Other tropical fruit"),
        ("Other vegetables", "Other vegetables"),
        ("Onion", "Onion"),
        ("Tomato", "Tomato"),
        ("corn", "Maize"),
        ("SOYB", "Soybean"),
    ],
)
def test_normalize_commodity_aliases(value, expected):
    assert normalize_commodity(value) == expected


def test_normalize_commodity_rejects_unknown():
    assert normalize_commodity("Atlantis") is None


def test_resolve_commodity_prefers_commodity_field():
    assert resolve_commodity({"commodity": "Rubber"}) == "Rubber"


def test_resolve_commodity_accepts_legacy_code_field():
    assert resolve_commodity({"commodity_code": "RUBB"}) == "Rubber"


def test_allowed_commodities_match_frontend_list():
    assert ALLOWED_COMMODITIES == sorted(FRONTEND_COMMODITY_NAMES)


@pytest.mark.parametrize("commodity", FRONTEND_COMMODITY_NAMES)
def test_endpoint_validator_accepts_frontend_commodity(client, commodity):
    pg_patch, ev_patch = _patch_psycopg2([])
    with pg_patch, ev_patch:
        resp = client.post(
            ENDPOINT,
            data=json.dumps(
                {
                    "locations": [
                        {
                            "unique_id": "c1",
                            "lat": 46.848091,
                            "lng": -67.891377,
                            "radius": 120,
                            "radius_units": "km",
                            "commodity": commodity,
                            "irrigation": "All",
                            "total_volume": 1000,
                        }
                    ]
                }
            ),
            content_type="application/json",
        )
    assert resp.status_code == 200, json.loads(resp.data)
