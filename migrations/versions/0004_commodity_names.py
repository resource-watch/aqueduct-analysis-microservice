"""Rename crop_production_pfaf.commodity_code to commodity (display names).

The reference CSV still ships IFPRI short codes (BANA, SOYB, …). This
migration renames the column and rewrites existing rows to canonical
display names (Banana, Soybean, …) so API queries match on names.

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-07

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

# Keep in sync with aqueduct/services/supply_chain_data/inputs_ifpri_croplist.csv
# (short_name -> title-cased full_name). Inlined here because Alembic loads
# migration modules without the application package on sys.path.
_CODE_TO_NAME = {
    "WHEA": "Wheat",
    "RICE": "Rice",
    "MAIZ": "Maize",
    "BARL": "Barley",
    "PMIL": "Pearl Millet",
    "SMIL": "Small Millet",
    "SORG": "Sorghum",
    "OCER": "Other Cereals",
    "POTA": "Potato",
    "SWPO": "Sweet Potato",
    "YAMS": "Yams",
    "CASS": "Cassava",
    "ORTS": "Other Roots",
    "BEAN": "Bean",
    "CHIC": "Chickpea",
    "COWP": "Cowpea",
    "PIGE": "Pigeonpea",
    "LENT": "Lentil",
    "OPUL": "Other Pulses",
    "SOYB": "Soybean",
    "GROU": "Groundnut",
    "CNUT": "Coconut",
    "OILP": "Oilpalm",
    "SUNF": "Sunflower",
    "RAPE": "Rapeseed",
    "SESA": "Sesameseed",
    "OOIL": "Other Oil Crops",
    "SUGC": "Sugarcane",
    "SUGB": "Sugarbeet",
    "COTT": "Cotton",
    "OFIB": "Other Fibre Crops",
    "ACOF": "Arabica Coffee",
    "RCOF": "Robusta Coffee",
    "COCO": "Cocoa",
    "TEAS": "Tea",
    "TOBA": "Tobacco",
    "BANA": "Banana",
    "PLNT": "Plantain",
    "TROF": "Tropical Fruit",
    "TEMF": "Temperate Fruit",
    "VEGE": "Vegetables",
    "REST": "Rest Of Crops",
}


def _code_to_name_case_sql(column: str) -> str:
    parts = [
        f"WHEN UPPER({column}) = '{code}' THEN '{name.replace(chr(39), chr(39) * 2)}'"
        for code, name in sorted(_CODE_TO_NAME.items())
    ]
    return (
        "CASE\n"
        + "\n".join(f"            {part}" for part in parts)
        + f"\n            ELSE {column}\n        END"
    )


def _name_to_code_case_sql(column: str) -> str:
    parts = [
        f"WHEN {column} = '{name.replace(chr(39), chr(39) * 2)}' THEN '{code}'"
        for code, name in sorted(_CODE_TO_NAME.items())
    ]
    return (
        "CASE\n"
        + "\n".join(f"            {part}" for part in parts)
        + f"\n            ELSE {column}\n        END"
    )


def upgrade():
    case_sql = _code_to_name_case_sql("commodity")
    op.execute(
        "ALTER TABLE crop_production_pfaf "
        "RENAME COLUMN commodity_code TO commodity"
    )
    op.execute(f"UPDATE crop_production_pfaf SET commodity = {case_sql}")
    op.execute("DROP INDEX IF EXISTS crop_prod_lookup_idx")
    op.execute(
        "CREATE INDEX crop_prod_lookup_idx "
        "ON crop_production_pfaf (commodity, irrigation, pfaf_id)"
    )


def downgrade():
    case_sql = _name_to_code_case_sql("commodity")
    op.execute("DROP INDEX IF EXISTS crop_prod_lookup_idx")
    op.execute(f"UPDATE crop_production_pfaf SET commodity = {case_sql}")
    op.execute(
        "ALTER TABLE crop_production_pfaf "
        "RENAME COLUMN commodity TO commodity_code"
    )
    op.execute(
        "CREATE INDEX crop_prod_lookup_idx "
        "ON crop_production_pfaf (commodity_code, irrigation, pfaf_id)"
    )
