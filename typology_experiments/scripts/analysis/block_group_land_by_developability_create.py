"""
This script creates analysis.block_group_land_by_developability
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


dvrpc_protected_land_and_water = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.dvrpc_protected_land_and_water"
)  # Uses my function to bring in the analysis.dvrpc_protected_land_and_water shapefile

block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_dvrpc_2020"
)  # Uses my function to bring in the analysis.block_groups_dvrpc_2020 shapefile


dvrpc_protected_land_and_water.insert(
    4, "dissolve_field", 1
)  # Adds a new column to the right of acres to just use for dissolving

undevelopable_land = dvrpc_protected_land_and_water.dissolve(
    by="dissolve_field"
)  # Dissolves all the individual polygons in dvrpc_protected_land_and_water so that it's just 1 big polygon of undevelopable land


undevelopable_block_group_fragments = gpd.overlay(
    block_groups_dvrpc_2020, undevelopable_land, how="intersection"
)  # Intersects block_groups_dvrpc_2020 with undevelopable_land

undevelopable_block_group_fragments[
    "developability"
] = 0  # Adds a column showing how these lands AREN'T developable

undevelopable_block_group_fragments = undevelopable_block_group_fragments[
    [
        "STATEFP",
        "COUNTYFP",
        "TRACTCE",
        "BLKGRPCE",
        "GEOID",
        "NAMELSAD",
        "ALAND",
        "AWATER",
        "developability",
        "geometry",
    ]
]  # Keeps only the columns I want and in the order I want them to be in

undevelopable_block_group_fragments = undevelopable_block_group_fragments.rename(
    columns={"geometry": "geom"}
)  # Renames the geometry column


developable_block_group_fragments = gpd.overlay(
    block_groups_dvrpc_2020, undevelopable_land, how="difference"
)  # Gets the difference between block_groups_dvrpc_2020 and undevelopable_land

developable_block_group_fragments[
    "developability"
] = 1  # Adds a column showing how these lands ARE developable

developable_block_group_fragments = developable_block_group_fragments[
    [
        "STATEFP",
        "COUNTYFP",
        "TRACTCE",
        "BLKGRPCE",
        "GEOID",
        "NAMELSAD",
        "ALAND",
        "AWATER",
        "developability",
        "geom",
    ]
]  # Keeps only the columns I want and in the order I want them to be in


block_group_land_by_developability = pd.concat(
    [undevelopable_block_group_fragments, developable_block_group_fragments]
)  # Merges/row binds/etc undevelopable_block_group_fragments and developable_block_group_fragments to get block_group_land_by_developability. This completes the manual unioning of

block_group_land_by_developability = block_group_land_by_developability.set_geometry(
    "geom"
)  # Tells Python the name of block_group_land_by_developability's geometry column

block_group_land_by_developability = block_group_land_by_developability.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries

db.import_geodataframe(
    block_group_land_by_developability, "block_group_land_by_developability", schema="analysis"
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
