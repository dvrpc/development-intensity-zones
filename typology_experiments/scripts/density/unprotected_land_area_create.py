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


dvrpc_2015_water_dissolved_clipped_to_block_groups = block_groups_dvrpc_2020.clip(
    dvrpc_2015_water_dissolved
)  # Clips the block groups to the dissolved water so that it"s now just a series of block groups that overlap with the water and have their shapes clipped to the parts of the water they overlap with

dvrpc_2020_pos_dissolved_clipped_to_block_groups = block_groups_dvrpc_2020.clip(
    dvrpc_2020_pos_dissolved
)  # Clips the block groups to the dissolved POS so that it"s now just a series of block groups that overlap with the POS and have their shapes clipped to the parts of the water they overlap with

dvrpc_2015_water_dissolved_clipped_to_block_groups[
    "water_sqmt"
] = (
    dvrpc_2015_water_dissolved_clipped_to_block_groups.area
)  # Calculates the area of each polygon in dvrpc_2015_water_dissolved_clipped_to_block_groups IN SQUARE METERS. Also, "water_sqmt" stands for "water square meters after clip"

dvrpc_2020_pos_dissolved_clipped_to_block_groups[
    "pos_sqmt"
] = (
    dvrpc_2020_pos_dissolved_clipped_to_block_groups.area
)  # Does the same but for dvrpc_2020_pos_dissolved_clipped_to_block_groups. Also, "pos_sqmt" stands for "POS square meters after clip"


dvrpc_2015_water_dissolved_clipped_to_block_groups = pd.DataFrame(
    dvrpc_2015_water_dissolved_clipped_to_block_groups[["GEOID", "water_sqmt"]]
)  # Makes dvrpc_2015_water_dissolved_clipped_to_block_groups non-spatial and only keeps the columns I want from it

dvrpc_2020_pos_dissolved_clipped_to_block_groups = pd.DataFrame(
    dvrpc_2020_pos_dissolved_clipped_to_block_groups[["GEOID", "pos_sqmt"]]
)  # Repeats the process but for dvrpc_2020_pos_dissolved_clipped_to_block_groups

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    dvrpc_2015_water_dissolved_clipped_to_block_groups, on=["GEOID"], how="left"
)  # Left joins dvrpc_2015_water_dissolved_clipped_to_block_groups to block_groups_dvrpc_2020, so that way each block group that overlapped with dvrpc_2015_water_dissolved gets their area in square meters that overlapped with that, and the block groups that didn"t overlap with that at all get no figures for water_sqmt

block_groups_dvrpc_2020["water_sqmt"] = block_groups_dvrpc_2020["water_sqmt"].fillna(
    0
)  # Replaces any NaN in water_sqmt with 0

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    dvrpc_2020_pos_dissolved_clipped_to_block_groups, on=["GEOID"], how="left"
)  # This and the next command repeat the process, but for dvrpc_2020_pos_dissolved_clipped_to_block_groups

block_groups_dvrpc_2020["pos_sqmt"] = block_groups_dvrpc_2020["pos_sqmt"].fillna(0)

block_groups_dvrpc_2020["undev_sqmt"] = (
    block_groups_dvrpc_2020["water_sqmt"] + block_groups_dvrpc_2020["pos_sqmt"]
)  # Calculates the total POS and water area of each block group in square meters (which is the total undevelopable area in square meters for each block group)

block_groups_dvrpc_2020[
    "ttl_sqmt"
] = (
    block_groups_dvrpc_2020.area
)  # Calculates the total area of each block group in square meters (IGNORES ALAND AND AWATER)

block_groups_dvrpc_2020["dev_sqmt"] = (
    block_groups_dvrpc_2020["ttl_sqmt"] - block_groups_dvrpc_2020["undev_sqmt"]
)  # Calculates the total non-POS and water area of each block group in square meters (which is the total developable area in square meters for each block group)

block_groups_dvrpc_2020["non_POS_water_acres"] = round(
    block_groups_dvrpc_2020["dev_sqmt"] / 4046.856, 0
)  # Converts the total non-POS and water area of each block group into acres, rounded to the nearest whole number

block_groups_dvrpc_2020["total_acres"] = round(
    block_groups_dvrpc_2020["ttl_sqmt"] / 4046.856, 0
)  # Converts the total area of each block group into acres, rounded to the nearest whole number


block_groups_dvrpc_2020 = block_groups_dvrpc_2020[
    [
        "STATEFP",
        "COUNTYFP",
        "TRACTCE",
        "BLKGRPCE",
        "GEOID",
        "NAMELSAD",
        "ALAND",
        "AWATER",
        "water_sqmt",
        "pos_sqmt",
        "undev_sqmt",
        "ttl_sqmt",
        "dev_sqmt",
        "non_POS_water_acres",
        "total_acres",
        "geom",
    ]
]  # Reorders the columns to be in the order I want them to be in

db.import_geodataframe(
    block_groups_dvrpc_2020, "unprotected_land_area", schema="density"
)  # Uploads the completed shapefile to density
