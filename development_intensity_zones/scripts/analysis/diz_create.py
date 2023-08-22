"""
This script uses Python to create analysis.diz
"""


import geopandas as gpd
import pandas as pd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


diz_bg = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.diz_block_group"
)  # Uses my function to bring in analysis.diz_block_group, and just the way it is originally at this point

pos_and_water = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.pos_h2o_diz_zone_0"
)  # Uses my function to bring in _raw.pos_h2o_diz_zone_0


diz_bg.insert(
    0, "dissolve", 1
)  # This and the next command essentially make a copy of diz_bg where all the block groups are dissolved into 1 big polygon

diz_bg_dis = diz_bg.dissolve(by="dissolve")

pos_and_water_reg = pos_and_water.clip(
    diz_bg_dis
)  # Clips pos_and_water to diz_bg_dis, so that pos_and_water doesn't go beyond the boundaries of the region


diz_bg_z0 = diz_bg[diz_bg["diz_zone"] == 0]  # Just gets the zone 0 (protected) parts of diz_bg

pos_and_water_reg_sep_from_z0 = gpd.overlay(
    pos_and_water_reg, diz_bg_z0, how="difference"
)  # Gets the parts of pos_and_water_reg which DON'T overlap with the zone 0 parts of diz_bg


diz_bg = diz_bg[
    ["diz_zone", "diz_zone_name", "geom"]
]  # Just keeps the columns I want from diz_bg that I still need, and in the order I want them to be in

diz_bg = diz_bg.dissolve(
    by=["diz_zone", "diz_zone_name"]
).reset_index()  # Dissolves diz_bg by 2 columns: diz_zone and diz_zone_name

pos_and_water_reg_sep_from_z0[
    "diz_zone_name"
] = "Protected"  # Adds a DIZ zone name column to pos_and_water_reg_sep_from_z0

pos_and_water_reg_sep_from_z0 = pos_and_water_reg_sep_from_z0.rename(
    columns={"zone": "diz_zone"}
)  # Renames zone to diz_zone in pos_and_water_reg_sep_from_z0

pos_and_water_reg_sep_from_z0 = pos_and_water_reg_sep_from_z0[
    ["diz_zone", "diz_zone_name", "geom"]
]  # Just keeps the columns I want from pos_and_water_reg_sep_from_z0 that I still need, and in the order I want them to be in

diz = pd.concat(
    [diz_bg, pos_and_water_reg_sep_from_z0]
)  # Unions/merges/row binds/etc diz_bg and pos_and_water_reg_sep_from_z0 to get what will become analysis.diz

diz = diz.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries

db.import_geodataframe(
    diz, "diz", schema="analysis"
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
