"""
This script technically unions analysis.diz_block_group with _raw.pos_h2o_diz_zone_0 to create analysis.diz 
"""

import pandas as pd
import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


diz_bg_1bigpolygon = db.get_geodataframe_from_query(
    "SELECT 1 AS dissolve, ST_UNION(geom) as geom FROM analysis.diz_block_group GROUP BY dissolve"
)  # Uses my function to bring in the analysis.diz_block_group feature class, but dissolved into 1 big polygon

pos_and_water = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.pos_h2o_diz_zone_0"
)  # Uses my function to bring in the _raw.pos_h2o_diz_zone_0 feature class, which is the new POS and water data

pos_and_water_within_region = pos_and_water.clip(
    diz_bg_1bigpolygon
)  # Clips pos_and_water to diz_bg_1bigpolygon, so that pos_and_water doesn't go beyond the boundaries of the region


diz_bg = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.diz_block_group"
)  # Uses my function to bring in the analysis.diz_block_group feature class, this time just the way it is originally

diz = gpd.overlay(
    pos_and_water, diz_bg, how="intersection"
)  # Intersects pos_and_water with diz_bg to create the final diz

db.import_geodataframe(
    diz, "diz", schema="analysis"
)  # Uploads the completed shapefile to analysis AS A REGULAR SPATIAL TABLE
