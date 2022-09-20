"""
This script creates the unprotected land area shapefile needed to conduct the density analysis and uploads it to density
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


dvrpc_2015_water = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.dvrpc_2015_water"
)  # Uses my function to bring in the _raw.dvrpc_2015_water shapefile. Also, acres is the water area in acres

dvrpc_2020_pos = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.dvrpc_2020_pos"
)  # Uses my function to bring in the _raw.dvrpc_2020_pos shapefile

block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM density.block_groups_dvrpc_2020"
)  # Uses my function to bring in the density.block_groups_dvrpc_2020 shapefile


dvrpc_2015_water_dissolved = dvrpc_2015_water.dissolve(
    by="lu15catn"
)  # Dissolves all the individual water body polygons in dvrpc_2015_water so that it"s just 1 big polygon of water bodies

dvrpc_2020_pos.insert(
    4, "land_type", "POS"
)  # Adds a new column to the right of os_type just to use for dissolving that shows how all the parcels are POS ones

dvrpc_2020_pos_dissolved = dvrpc_2020_pos.dissolve(
    by="land_type"
)  # Dissolves all the individual POS parcel polygons in dvrpc_2020_pos so that it"s just 1 big polygon of POS parcel polygons


b
