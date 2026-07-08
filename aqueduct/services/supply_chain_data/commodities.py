"""Crop commodity names and legacy IFPRI code mappings.

Reference data is keyed by display names (e.g. ``Banana``) rather than
four-letter codes (``BANA``). Callers may still supply legacy codes or
common aliases; ``normalize_commodity`` resolves them to the canonical
name stored in ``crop_production_pfaf.commodity``.
"""

from __future__ import annotations

# Canonical display names accepted from the supply-chain frontend template.
FRONTEND_COMMODITY_NAMES = (
    "Arabica coffee",
    "Banana",
    "Barley",
    "Bean",
    "Cassava",
    "Chickpea",
    "Citrus",
    "Cocoa",
    "Coconut",
    "Cotton",
    "Cowpea",
    "Groundnut",
    "Lentil",
    "Maize",
    "Oilpalm",
    "Onion",
    "Other cereals",
    "Other fibres",
    "Other oil crops",
    "Other pulses",
    "Other roots",
    "Other tropical fruit",
    "Other vegetables",
    "Pearl millet",
    "Pigeon pea",
    "Plantain",
    "Potato",
    "Rapeseed",
    "Rest of crops",
    "Rice",
    "Robusta coffee",
    "Rubber",
    "Sesame seed",
    "Small millet",
    "Sorghum",
    "Soybean",
    "Sugarbeet",
    "Sugarcane",
    "Sunflower",
    "Sweet potato",
    "Tea",
    "Temperate fruit",
    "Tobacco",
    "Tomato",
    "Wheat",
    "Yams",
)

# (IFPRI/CSV code, canonical display name, *extra lowercase aliases)
_COMMODITY_DEFINITIONS: tuple[tuple[str, str, ...], ...] = (
    ("WHEA", "Wheat", "wheat"),
    ("RICE", "Rice", "rice"),
    ("MAIZ", "Maize", "maize", "corn"),
    ("BARL", "Barley", "barley"),
    ("PMIL", "Pearl millet", "pearl millet"),
    ("SMIL", "Small millet", "small millet"),
    ("MILL", "Small millet", "mill"),
    ("SORG", "Sorghum", "sorghum", "sorghum grain"),
    ("OCER", "Other cereals", "other cereals", "oats", "rye"),
    ("POTA", "Potato", "potato"),
    ("SWPO", "Sweet potato", "sweet potato"),
    ("YAMS", "Yams", "yams"),
    ("CASS", "Cassava", "cassava", "tapioca"),
    ("ORTS", "Other roots", "other roots"),
    ("BEAN", "Bean", "bean"),
    ("CHIC", "Chickpea", "chickpea"),
    ("COWP", "Cowpea", "cowpea"),
    ("PIGE", "Pigeon pea", "pigeon pea", "pigeonpea"),
    ("LENT", "Lentil", "lentil"),
    ("OPUL", "Other pulses", "other pulses"),
    ("SOYB", "Soybean", "soybean", "soy", "soya", "soyabean", "soyabeans", "soybeans", "soybean meal"),
    ("GROU", "Groundnut", "groundnut"),
    ("CNUT", "Coconut", "coconut"),
    ("OILP", "Oilpalm", "oilpalm", "palm"),
    ("SUNF", "Sunflower", "sunflower"),
    ("RAPE", "Rapeseed", "rapeseed", "canola", "canola oil"),
    ("SESA", "Sesame seed", "sesame seed", "sesameseed"),
    ("OOIL", "Other oil crops", "other oil crops"),
    ("SUGC", "Sugarcane", "sugarcane", "sugar cane"),
    ("SUGB", "Sugarbeet", "sugarbeet"),
    ("COTT", "Cotton", "cotton"),
    ("OFIB", "Other fibres", "other fibres", "other fibre crops", "flaxseed"),
    ("ACOF", "Arabica coffee", "arabica coffee", "acof"),
    ("RCOF", "Robusta coffee", "robusta coffee", "rcof"),
    ("COFF", "Robusta coffee", "coff", "coffee"),
    ("COCO", "Cocoa", "cocoa"),
    ("TEAS", "Tea", "tea"),
    ("TOBA", "Tobacco", "tobacco"),
    ("BANA", "Banana", "banana"),
    ("PLNT", "Plantain", "plantain"),
    ("TROF", "Other tropical fruit", "other tropical fruit", "tropical fruit"),
    ("TEMF", "Temperate fruit", "temperate fruit"),
    ("VEGE", "Other vegetables", "other vegetables", "vegetables"),
    ("REST", "Rest of crops", "rest of crops"),
    ("CITR", "Citrus", "citrus", "citr"),
    ("ONIO", "Onion", "onion", "onio"),
    ("RUBB", "Rubber", "rubber", "rubb"),
    ("TOMA", "Tomato", "tomato", "toma"),
)

# Extra aliases -> code (when alias does not belong to a single definition row).
_EXTERNAL_ALIASES = {}


def _build_mappings() -> tuple[dict[str, str], dict[str, str]]:
    code_to_name: dict[str, str] = {}
    lookup: dict[str, str] = {}

    for definition in _COMMODITY_DEFINITIONS:
        code = definition[0].upper()
        name = definition[1]
        code_to_name[code] = name
        lookup[name.lower()] = name
        lookup[code.lower()] = name
        for alias in definition[2:]:
            lookup[alias.lower()] = name

    for alias, code in _EXTERNAL_ALIASES.items():
        name = code_to_name.get(code.upper())
        if name:
            lookup[alias.lower()] = name

    return code_to_name, lookup


CODE_TO_NAME, _LOOKUP = _build_mappings()
ALLOWED_COMMODITIES = sorted(FRONTEND_COMMODITY_NAMES)


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
    return (
        "CASE\n"
        + "\n".join(f"            {part}" for part in parts)
        + f"\n            ELSE {column}\n        END"
    )
