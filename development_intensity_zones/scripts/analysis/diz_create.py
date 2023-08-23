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

diff1 = gpd.overlay(
    pos_and_water_reg, diz_bg_z0, how="difference"
)  # Gets the parts of pos_and_water_reg which DON'T overlap with the zone 0 parts of diz_bg

diff1["diz_zone_name"] = "Protected"  # Adds a DIZ zone name column to diff1

diff1 = diff1.rename(columns={"zone": "diz_zone"})  # Renames zone to diz_zone in diff1

diff1 = diff1[
    ["diz_zone", "diz_zone_name", "geom"]
]  # Just keeps the columns I want from diff1 that I still need, and in the order I want them to be in


diff2 = gpd.overlay(
    diz_bg_z0, diff1, how="difference"
)  # Gets the parts of diz_bg_z0 which DON'T overlap with the 1st geographic difference obtained (diff1) to obtain the 2nd geographic difference (diff2)

diff2 = diff2[
    ["diz_zone", "diz_zone_name", "geom"]
]  # Just keeps the columns I want from diff2 that I still need, and in the order I want them to be in

diff2 = diff2.dissolve(
    by=["diz_zone", "diz_zone_name"]
).reset_index()  # Dissolves diff2 by 2 columns: diz_zone and diz_zone_name


diz = pd.concat(
    [diff2, diff1]
)  # Unions/merges/row binds/etc diff2 and diff1 to get what will become analysis.diz (I don't think the order of objects matters, it just made more sense in my head spatially to write the order of objects this way)

diz = diz.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries

db.import_geodataframe(
    diz, "diz", schema="analysis"
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
