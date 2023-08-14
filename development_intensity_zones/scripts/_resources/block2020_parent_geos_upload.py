"""
This script uploads _resources.block2020_parent_geos
"""

import pandas as pd
import gspread
import re


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


block2020_parent_geos = pd.read_excel(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/development_intensity_zones/block2020_parent_geos.xlsx"
)  # Reads in block2020_parent_geos

block2020_parent_geos[
    list(filter(re.compile(r"_id").search, list(block2020_parent_geos.columns)))
] = block2020_parent_geos[
    list(filter(re.compile(r"_id").search, list(block2020_parent_geos.columns)))
].astype(
    str
)  # Makes all columns that have "_id" in their names string


db.import_dataframe(
    block2020_parent_geos,
    "_resources.block2020_parent_geos",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports block2020_parent_geos
