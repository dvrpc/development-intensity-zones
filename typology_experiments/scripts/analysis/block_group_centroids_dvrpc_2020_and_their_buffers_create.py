"""
This script creates analysis.block_group_centroids_dvrpc_2020 and analysis.block_group_centroid_buffers_dvrpc_2020
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_dvrpc_2020"
)  # Uses my function to bring in the analysis.block_groups_dvrpc_2020 shapefile

block_group_centroids_dvrpc_2020 = (
    block_groups_dvrpc_2020.copy()
)  # Begins the process of getting the block groups' centroids

block_group_centroids_dvrpc_2020["geom"] = block_group_centroids_dvrpc_2020[
    "geom"
].centroid  # Finishes the process of getting the block groups' centroids


block_group_centroids_dvrpc_2020_buffers_2_mile = (
    block_group_centroids_dvrpc_2020.copy()
)  # Begins the process of getting all the block group centroids' 2 mile buffers

block_group_centroids_dvrpc_2020_buffers_2_mile[
    "geom"
] = block_group_centroids_dvrpc_2020_buffers_2_mile["geom"].buffer(
    3218.69
)  # Finishes the process of getting all the block group centroids' 2 mile buffers. AND NOTE THAT 3218.69 METERS IS 2 MILES

block_group_centroids_dvrpc_2020_buffers_2_mile.insert(
    8, "buff_mi", 2
)  # Adds a column showing these buffers are 2 miles wide, and puts it to the left of geom

block_group_centroids_dvrpc_2020_buffers_5_mile = (
    block_group_centroids_dvrpc_2020.copy()
)  # Begins the process of getting all the block group centroids' 2 mile buffers

block_group_centroids_dvrpc_2020_buffers_5_mile[
    "geom"
] = block_group_centroids_dvrpc_2020_buffers_5_mile["geom"].buffer(
    8046.72
)  # Finishes the process of getting all the block group centroids' 2 mile buffers. AND NOTE THAT 8046.72 METERS IS 5 MILES

block_group_centroids_dvrpc_2020_buffers_5_mile.insert(
    8, "buff_mi", 5
)  # Adds a column showing these buffers are 5 miles wide, and puts it to the left of geom

block_group_centroid_buffers_dvrpc_2020 = pd.concat(
    [
        block_group_centroids_dvrpc_2020_buffers_2_mile,
        block_group_centroids_dvrpc_2020_buffers_5_mile,
    ]
)  # Merges/row binds/etc block_group_centroids_dvrpc_2020_buffers_2_mile and block_group_centroids_dvrpc_2020_buffers_5_mile to get block_group_centroid_buffers_dvrpc_2020


shps_to_upload_dictionary = {
    "block_group_centroids_dvrpc_2020": block_group_centroids_dvrpc_2020,
    "block_group_centroid_buffers_dvrpc_2020": block_group_centroid_buffers_dvrpc_2020,
}  # Creates the dictionary of the shapefiles to upload to the database, where the keys are their names and the values are the shapefiles themselves

[
    db.import_geodataframe(shp, f"{shpname}", schema="analysis")
    for shpname, shp in shps_to_upload_dictionary.items()
]  # For each shapefile in shps_to_upload_dictionary, exports it to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
