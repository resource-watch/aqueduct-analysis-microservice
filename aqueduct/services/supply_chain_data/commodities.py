"""Crop commodity names and legacy IFPRI code mappings.

Reference data is keyed by display names (e.g. ``Banana``) rather than
four-letter codes (``BANA``). Callers may still supply legacy codes or
common aliases; ``normalize_commodity`` resolves them to the canonical
name stored in ``crop_production_pfaf.commodity``.
"""

from __future__ import annotations

import csv
from pathlib import Path

_CROPLIST_PATH = Path(__file__).with_name("inputs_ifpri_croplist.csv")

# Common caller aliases -> IFPRI short code (mirrors food_supply_chain_service).
_COMMODITY_ALIASES = {
    "corn": "maiz",
    "canola": "rape",
    "canola oil": "rape",
    "flaxseed": "ofib",
    "oats": "ocer",
    "rye": "ocer",
    "palm": "oilp",
    "sorghum grain": "sorg",
    "soy": "soyb",
    "soya": "soyb",
    "soyabean": "soyb",
    "soybean meal": "soyb",
    "soyabeans": "soyb",
    "soybeans": "soyb",
    "sugar cane": "sugc",
    "tapioca": "cass",
}


def _canonical_name(full_name: str) -> str:
    return full_name.strip().title()


def _load_mappings() -> tuple[dict[str, str], dict[str, str]]:
    code_to_name: dict[str, str] = {}
    lookup: dict[str, str] = {}

    with _CROPLIST_PATH.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            code = row["short_name"].strip().upper()
            name = _canonical_name(row["full_name"])
            code_to_name[code] = name
            lookup[row["full_name"].strip().lower()] = name
            lookup[code.lower()] = name
            lookup[name.lower()] = name

    for alias, code in _COMMODITY_ALIASES.items():
        name = code_to_name.get(code.upper())
        if name:
            lookup[alias.lower()] = name

    return code_to_name, lookup


CODE_TO_NAME, _LOOKUP = _load_mappings()
ALLOWED_COMMODITIES = sorted(set(CODE_TO_NAME.values()))


def normalize_commodity(value: str | None) -> str | None:
    """Resolve a crop name, alias, or legacy code to the canonical name."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return _LOOKUP.get(text.lower())


def resolve_commodity(loc: dict) -> str | None:
    """Read ``commodity`` or legacy ``commodity_code`` from a location dict."""
    raw = loc.get("commodity")
    if raw is None:
        raw = loc.get("commodity_code")
    if raw is None:
        return None
    return normalize_commodity(str(raw))


def code_to_name_case_sql(column: str) -> str:
    """Build a SQL ``CASE`` that maps legacy codes in ``column`` to names."""
    parts = [
        f"WHEN UPPER({column}) = '{code}' THEN '{name.replace(chr(39), chr(39) * 2)}'"
        for code, name in sorted(CODE_TO_NAME.items())
    ]
    return "CASE\n" + "\n".join(f"            {part}" for part in parts) + f"\n            ELSE {column}\n        END"
