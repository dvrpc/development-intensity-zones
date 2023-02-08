"""
This script intersects _raw.pos_h2o_transect_zone_0 with analysis.transect using geopandas as a test 
"""


import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


transect_1bigpolygon = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.transect"
)  # Uses my function to bring in the analysis.transect shapefile, but this version will eventually get dissolved into 1 big polygon

transect_1bigpolygon.insert(
    0, "dissolve_field", 1
)  # Adds a field just so that it can be dissolved into 1 big polygon

transect_1bigpolygon = transect_1bigpolygon.dissolve(
    by="dissolve_field"
)  # Dissolves all the individual polygons so that it's just 1 big polygon


pos_and_water = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.pos_h2o_transect_zone_0"
)  # Uses my function to bring in the _raw.pos_h2o_transect_zone_0 shapefile, which is the new POS and water data

pos_and_water_within_region = pos_and_water.clip(
    transect_1bigpolygon
)  # Clips pos_and_water to transect_1bigpolygon, so that pos_and_water doesn't go beyond the boundaries of the region


transect = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.transect"
)  # Uses my function to bring in the analysis.transect shapefile

pos_and_water_intersection_with_transect = gpd.overlay(
    pos_and_water, transect, how="intersection"
)  # Intersects pos_and_water with transect

pos_and_water_intersection_with_transect.to_file(
    "C:/Users/ISchwarzenberg/Downloads/pos_and_water_intersection_with_transect.shp"
)
