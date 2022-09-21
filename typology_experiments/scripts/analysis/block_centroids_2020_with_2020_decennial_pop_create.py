"""
This script creates analysis.block_centroids_2020_with_2020_decennial_pop
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


blocks_all4states_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.tl_2020_10_tabblock20 UNION SELECT * FROM _raw.tl_2020_24_tabblock20 UNION SELECT * FROM _raw.tl_2020_34_tabblock20 UNION SELECT * FROM _raw.tl_2020_42_tabblock20"
)  # Uses my function to bring in the 2020 Delaware, Maryland, New Jersey and Pennsylvania blocks shapefiles, and combine them into 1 big one

blocks_all4states_2020_centroids = (
    blocks_all4states_2020.copy()
)  # Begins the process of getting all the blocks' centroids

blocks_all4states_2020_centroids["geom"] = blocks_all4states_2020_centroids[
    "geom"
].centroid  # Finishes the process of getting all the blocks' centroids


tot_pops_and_hhs_2020_block = db.get_dataframe_from_query(
    'SELECT CONCAT(state,county,tract,block) AS "GEOID20", p1_001n AS t_pop_d20, p5_001n AS gq_pop_d20 FROM _raw.tot_pops_and_hhs_2020_block'
)  # Uses my function to bring in the 2020 Decennial population by block data


block_centroids_2020_with_2020_decennial_pop = blocks_all4states_2020_centroids.merge(
    tot_pops_and_hhs_2020_block, on=["GEOID20"], how="left"
)  # Left joins tot_pops_and_hhs_2020_block to blocks_all4states_2020_centroids to create block_centroids_2020_with_2020_decennial_pop

block_centroids_2020_with_2020_decennial_pop = block_centroids_2020_with_2020_decennial_pop[
    [
        "STATEFP20",
        "COUNTYFP20",
        "TRACTCE20",
        "BLOCKCE20",
        "GEOID20",
        "NAME20",
        "MTFCC20",
        "UR20",
        "UACE20",
        "UATYPE20",
        "FUNCSTAT20",
        "ALAND20",
        "AWATER20",
        "INTPTLAT20",
        "INTPTLON20",
        "HOUSING20",
        "POP20",
        "t_pop_d20",
        "gq_pop_d20",
        "geom",
        "uid",
    ]
]  # Reorders the columns

db.import_geodataframe(
    block_centroids_2020_with_2020_decennial_pop,
    "block_centroids_2020_with_2020_decennial_pop",
    schema="analysis",
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
