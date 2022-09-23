"""
This script creates analysis.block_groups_dvrpc_2020
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


block_groups_all_of_nj_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.tl_2020_34_bg"
)  # Loads in all of NJ's block groups

block_groups_all_of_pa_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.tl_2020_42_bg"
)  # Loads in all of PA's block groups


block_groups_dvrpc_nj_2020 = block_groups_all_of_nj_2020[
    block_groups_all_of_nj_2020["COUNTYFP"].isin(["005", "007", "015", "021"])
]  # Gets just the NJ block groups in the DVRPC region

block_groups_dvrpc_pa_2020 = block_groups_all_of_pa_2020[
    block_groups_all_of_pa_2020["COUNTYFP"].isin(["017", "029", "045", "091", "101"])
]  # Gets just the PA block groups in the DVRPC region


block_groups_dvrpc_2020 = pd.concat(
    [block_groups_dvrpc_nj_2020, block_groups_dvrpc_pa_2020]
)  # Merges/row binds/etc block_groups_dvrpc_nj_2020 and block_groups_dvrpc_pa_2020 to get block_groups_dvrpc_2020


block_groups_dvrpc_2020["dissolve_field"] = block_groups_dvrpc_2020[
    "GEOID"
]  # This and the next command make it so it's 1 record per GEOID in block_groups_dvrpc_2020

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.dissolve(by="dissolve_field")


block_groups_dvrpc_2020 = block_groups_dvrpc_2020.explode(
    ignore_index=True
)  # Makes it so it's just regular polygons

db.import_geodataframe(
    block_groups_dvrpc_2020, "block_groups_dvrpc_2020", schema="analysis"
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
