"""
This script creates analysis.unprotected_land_area
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


developable_block_group_fragments = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_group_land_by_developability WHERE developability=1"
)  # Uses my function to bring in the developable records/polygons of the analysis.block_group_land_by_developability shapefile

block_groups_24co_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_24co_2020"
)  # Uses my function to bring in the analysis.block_groups_24co_2020 shapefile

block_groups_24co_2020 = block_groups_24co_2020.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries


developable_block_group_fragments["acres"] = round(
    developable_block_group_fragments.area / 4046.856, 0
)  # Calculates the area of each polygon in developable_block_group_fragments IN ACRES and rounds those figures to the nearest whole number

data_for_non_pos_water_acres_column = (
    developable_block_group_fragments.groupby(["GEOID"], as_index=False)
    .agg({"acres": "sum"})
    .rename(columns={"acres": "non_pos_water_acres"})
)  # For each GEOID (block group ID/block group), gets the total acreage of developable land/total non-POS and water acres

unprotected_land_area = pd.merge(
    block_groups_24co_2020, data_for_non_pos_water_acres_column, on=["GEOID"], how="left"
)  # Left joins data_for_non_pos_water_acres_column to block_groups_24co_2020

unprotected_land_area["aland_acres"] = round(
    unprotected_land_area["ALAND"] / 4046.856, 0
)  # Converts ALAND to acres and rounds those figures to the nearest whole number

unprotected_land_area = unprotected_land_area[
    [
        "GEOID",
        "ALAND",
        "non_pos_water_acres",
        "aland_acres",
        "geom",
    ]
]  # Reorders the columns to be in the order I want them to be in

db.import_geodataframe(
    unprotected_land_area, "unprotected_land_area", schema="analysis"
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
