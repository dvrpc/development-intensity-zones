"""
This script technically unions analysis.diz_block_group with _raw.pos_h2o_diz_zone_0 to create analysis.diz 
"""

import pandas as pd
import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


diz_bg = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.diz_block_group"
)  # Uses my function to bring in the analysis.diz_block_group feature class, this time just the way it is originally

diz_bg.insert(
    0, "dissolve", 1
)  # This and the next command essentially make a copy of diz_bg where all the block groups are dissolved into 1 big polygon

diz_bg_dissolved = diz_bg.dissolve(by="dissolve")

pos_and_water = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.pos_h2o_diz_zone_0"
)  # Uses my function to bring in the _raw.pos_h2o_diz_zone_0 feature class, which is the new POS and water data

pos_and_water_within_region = pos_and_water.clip(
    diz_bg_dissolved
)  # Clips pos_and_water to diz_bg_dissolved, so that pos_and_water doesn't go beyond the boundaries of the region


diz = gpd.overlay(
    pos_and_water_within_region, diz_bg, how="intersection"
)  # Intersects pos_and_water_within_region with diz_bg to create the final diz

diz = diz.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries

db.import_geodataframe(
    diz, "diz", schema="analysis"
)  # Uploads the completed shapefile to analysis AS A REGULAR SPATIAL TABLE
