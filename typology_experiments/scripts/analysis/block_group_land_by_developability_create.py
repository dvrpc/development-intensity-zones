"""
This script creates analysis.block_group_land_by_developability
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


undevelopable_area = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.pos_h2o_diz_zone_0"
)  # Uses my function to bring in the _raw.pos_h2o_diz_zone_0 shapefile, which is the undevelopable part of the study area

block_groups_24co_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_24co_2020"
)  # Uses my function to bring in the analysis.block_groups_24co_2020 shapefile


undevelopable_block_group_fragments = gpd.overlay(
    block_groups_24co_2020, undevelopable_area, how="intersection"
)  # Intersects block_groups_24co_2020 with undevelopable_area

undevelopable_block_group_fragments[
    "developability"
] = 0  # Adds a column showing how these lands AREN'T developable

undevelopable_block_group_fragments = undevelopable_block_group_fragments[
    [
        "GEOID",
        "ALAND",
        "developability",
        "geometry",
    ]
]  # Keeps only the columns I want and in the order I want them to be in

undevelopable_block_group_fragments = undevelopable_block_group_fragments.rename(
    columns={"geometry": "geom"}
)  # Renames the geometry column so that it can later be merged/row binded/etc with developable_block_group_fragments


developable_block_group_fragments = gpd.overlay(
    block_groups_24co_2020, undevelopable_area, how="difference"
)  # Gets the difference between block_groups_24co_2020 and undevelopable_area

developable_block_group_fragments[
    "developability"
] = 1  # Adds a column showing how these lands ARE developable

developable_block_group_fragments = developable_block_group_fragments[
    [
        "GEOID",
        "ALAND",
        "developability",
        "geom",
    ]
]  # Keeps only the columns I want and in the order I want them to be in


block_group_land_by_developability = pd.concat(
    [undevelopable_block_group_fragments, developable_block_group_fragments]
)  # Merges/row binds/etc undevelopable_block_group_fragments and developable_block_group_fragments to get block_group_land_by_developability. This completes the manual unioning of

block_group_land_by_developability = block_group_land_by_developability.rename(
    columns={"geom": "geometry"}
)  # Renames the geometry column because GEOPANDAS PREFERS THE GEOMETRY COLUMN BE CALLED geometry, THIS SOLVES ERRORS LATER

block_group_land_by_developability = block_group_land_by_developability.set_geometry(
    "geometry"
)  # Tells Python the name of block_group_land_by_developability's geometry column

block_group_land_by_developability = block_group_land_by_developability.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries

db.import_geodataframe(
    block_group_land_by_developability, "block_group_land_by_developability", schema="analysis"
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
