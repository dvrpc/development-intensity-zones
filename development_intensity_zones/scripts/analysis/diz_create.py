"""
This script uses PYTHON to create analysis.diz
"""


import geopandas as gpd
import pandas as pd
import numpy as np


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


diz_bg = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.diz_block_group"
)  # Uses my function to bring in the analysis.diz_block_group feature class, this time just the way it is originally


diz_bg.insert(
    0, "dissolve", 1
)  # This and the next command essentially make a copy of diz_bg where all the block groups are dissolved into 1 big polygon

diz_bg_dis = diz_bg.dissolve(by="dissolve")

pos_and_water = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.pos_h2o_diz_zone_0"
)  # Uses my function to bring in the _raw.pos_h2o_diz_zone_0 feature class, which is the new POS and water data

pos_and_water_reg = pos_and_water.clip(
    diz_bg_dis
)  # Clips pos_and_water to diz_bg_dis, so that pos_and_water doesn't go beyond the boundaries of the region


diz_bg_z0 = diz_bg[diz_bg["diz_zone"] == 0]  # Just gets the zone 0 (protected) parts of diz_bg

pos_and_water_reg_sep_from_z0 = gpd.overlay(
    pos_and_water_reg, diz_bg_z0, how="difference"
)  # Gets the parts of pos_and_water_reg which DON'T overlap with the zone 0 parts of diz_bg


diz_bg = diz_bg.drop(
    columns=["dissolve", "row_number"]
)  # Drops any columns that are no longer needed from diz_bg

pos_and_water_reg_sep_from_z0 = pos_and_water_reg_sep_from_z0.rename(
    columns={"zone": "diz_zone"}
)  # Renames zone to diz_zone in pos_and_water_reg_sep_from_z0

pos_and_water_reg_sep_from_z0 = pos_and_water_reg_sep_from_z0.assign(
    block_group20=[""],
    density_index=[np.nan],
    proximity_index=[np.nan],
    density_index_level=[""],
    proximity_index_level=[""],
    prelim_diz_zone=[np.nan],
    crosswalk_density=[np.nan],
    average_comm_stories=[np.nan],
    crosswalk_bonus=[np.nan],
    stories_bonus=[np.nan],
    diz_zone_name=["Protected"],
)  # Adds the columns that diz_big has but pos_and_water_reg_sep_from_z0 doesn't have to pos_and_water_reg_sep_from_z0

pos_and_water_reg_sep_from_z0 = pos_and_water_reg_sep_from_z0[
    list(diz_bg.columns)
]  # Keeps the same columns diz_bg has in pos_and_water_reg_sep_from_z0, and in the same order

diz = pd.concat(
    [diz_bg, pos_and_water_reg_sep_from_z0]
)  # Unions/merges/row binds/etc diz_bg and pos_and_water_reg_sep_from_z0 to get what will become analysis.diz

diz = diz.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries

db.import_geodataframe(
    diz, "diz", schema="analysis"
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
