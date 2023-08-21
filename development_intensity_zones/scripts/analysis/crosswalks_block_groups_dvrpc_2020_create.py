"""
This script creates analysis.crosswalks_block_groups_dvrpc_2020 (originally this was done in SQL, but for some reason it took much longer for the SQL script to run than this Python script to run)
"""

import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


crosswalks = db.get_geodataframe_from_query(
    "SELECT 1 AS sj_val, shape AS geom FROM _raw.crosswalks"
).to_crs(
    26918
)  # Brings in the crosswalks, with just the columns I want and in the order I want them to be in, and in the CRS I want it to be in. NOTE THAT THE GEOMETRY COLUMN NEEDED TO BE REFERENCED AS "shape AS geom" IN ORDER FOR THIS TO WORK. Also, sj_val, which stands for "spatial join value", is just needed to get each block group's number of crosswalks as part of a spatial join later

bgs = db.get_geodataframe_from_query(
    'SELECT "GEOID", geom FROM analysis.block_groups_dvrpc_2020'
)  # Brings in the block groups, and just the columns I want and in the order I want them to be in


crosswalks_bgs_overlay = gpd.overlay(
    crosswalks,
    bgs,
    how="intersection",
)  # Gives each crosswalk the GEOID(s) of the block groups they're in

crosswalks_bgs_overlay["sj_val"] = crosswalks_bgs_overlay["sj_val"].fillna(
    0
)  # Makes any nulls in sj_val 0

crosswalks_bgs_dvrpc_2020 = (
    crosswalks_bgs_overlay.groupby(["GEOID"], as_index=False)
    .agg({"sj_val": "sum"})
    .rename(columns={"sj_val": "crosswalk_count"})
)  # For each block group, gets the total number of crosswalks


db.execute(
    "TRUNCATE analysis.crosswalks_block_groups_dvrpc_2020"
)  # First wipes out all of the records in analysis.crosswalks_block_groups_dvrpc_2020

db.import_dataframe(
    crosswalks_bgs_dvrpc_2020,
    "analysis.crosswalks_block_groups_dvrpc_2020",
    df_import_kwargs={"if_exists": "append", "index": False},
)  # Then repopulates analysis.crosswalks_block_groups_dvrpc_2020 with the new records
