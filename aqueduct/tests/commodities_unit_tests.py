"""Unit tests for commodity name/code normalization."""

import pytest

from aqueduct.services.supply_chain_data.commodities import (
    ALLOWED_COMMODITIES,
    normalize_commodity,
    resolve_commodity,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("Banana", "Banana"),
        ("banana", "Banana"),
        ("BANA", "Banana"),
        ("bana", "Banana"),
        ("Soybean", "Soybean"),
        ("SOYB", "Soybean"),
        ("corn", "Maize"),
        ("Maize", "Maize"),
        ("other oil crops", "Other Oil Crops"),
    ],
)
def test_normalize_commodity(value, expected):
    assert normalize_commodity(value) == expected


def test_normalize_commodity_rejects_unknown():
    assert normalize_commodity("Atlantis") is None


def test_resolve_commodity_prefers_commodity_field():
    assert resolve_commodity({"commodity": "Banana"}) == "Banana"


def test_resolve_commodity_accepts_legacy_code_field():
    assert resolve_commodity({"commodity_code": "BANA"}) == "Banana"


def test_allowed_commodities_are_sorted_unique_names():
    assert ALLOWED_COMMODITIES == sorted(ALLOWED_COMMODITIES)
    assert "Banana" in ALLOWED_COMMODITIES
    assert "BANA" not in ALLOWED_COMMODITIES
