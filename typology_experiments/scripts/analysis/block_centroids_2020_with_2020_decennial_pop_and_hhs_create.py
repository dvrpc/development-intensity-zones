"""
This script creates analysis.block_centroids_2020_with_2020_decennial_pop_and_hhs
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


blocks_all4states_2020_with_2020_decennial_pop_and_hhs = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.tl_2020_10_tabblock20 UNION SELECT * FROM _raw.tl_2020_24_tabblock20 UNION SELECT * FROM _raw.tl_2020_34_tabblock20 UNION SELECT * FROM _raw.tl_2020_42_tabblock20"
)  # Uses my function to bring in the 2020 Delaware, Maryland, New Jersey and Pennsylvania blocks shapefiles, and combine them into 1 big one


blocks_all4states_2020_with_2020_decennial_pop_and_hhs.insert(
    3, "bce20_1std", blocks_all4states_2020_with_2020_decennial_pop_and_hhs["BLOCKCE20"].str[0]
)  # Gets the first digit of each BLOCKCE20 value, puts those values into a new column, and puts that column to the left of BLOCKCE20

blocks_all4states_2020_with_2020_decennial_pop_and_hhs.insert(
    4,
    "block_group20",
    blocks_all4states_2020_with_2020_decennial_pop_and_hhs["STATEFP20"]
    + blocks_all4states_2020_with_2020_decennial_pop_and_hhs["COUNTYFP20"]
    + blocks_all4states_2020_with_2020_decennial_pop_and_hhs["TRACTCE20"]
    + blocks_all4states_2020_with_2020_decennial_pop_and_hhs["bce20_1std"],
)  # Gets each block's block group ID, puts those values into a new column, and puts that column to the right of bce20_1std


block_centroids_2020_with_2020_decennial_pop_and_hhs = (
    blocks_all4states_2020_with_2020_decennial_pop_and_hhs.copy()
)  # Begins the process of getting all the blocks' centroids

block_centroids_2020_with_2020_decennial_pop_and_hhs[
    "geom"
] = block_centroids_2020_with_2020_decennial_pop_and_hhs[
    "geom"
].centroid  # Finishes the process of getting all the blocks' centroids


db.import_geodataframe(
    block_centroids_2020_with_2020_decennial_pop_and_hhs,
    "block_centroids_2020_with_2020_decennial_pop_and_hhs",
    schema="analysis",
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
