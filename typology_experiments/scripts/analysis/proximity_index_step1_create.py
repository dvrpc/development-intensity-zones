"""
This script creates analysis.proximity_index_step1. ALSO, REMEMBER TO FIRST CASCADE DELETE/DROP CASCADE analysis.proximity_index_step1 BEFORE RUNNING THIS SCRIPT, AND THEN AFTER FINISHING RUNNING THIS SCRIPT, RERUN THE SCRIPTS THAT CREATE THE VIEWS WHICH DEPEND ON analysis.proximity_index_step1
"""


import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


land_block_group_geoids = [
    s
    for l in db.query('SELECT "GEOID" FROM analysis.block_groups_24co_2020 WHERE "ALAND" <> 0')
    for s in l
]  # First gets a list of the GEOIDs of the block groups on land

string_of_land_block_group_geoids = "', '".join(
    land_block_group_geoids
)  # Also puts those GEOIDs into a comma-separated string len(string_of_land_block_group_geoids)


buffers_2_mile = db.get_geodataframe_from_query(
    'SELECT * FROM analysis.incorp_del_river_bg_centroids_24co_2020_buffers WHERE buff_mi = 2 AND "GEOID" IN ('
    + "'"
    + string_of_land_block_group_geoids
    + "')"
)  # Uses my function to bring in the shapefile containing the 2020 LAND block group centroids' 2-mile buffers to use that Sean Lawrence created which incorporate the Delaware River

buffers_5_mile = db.get_geodataframe_from_query(
    'SELECT * FROM analysis.incorp_del_river_bg_centroids_24co_2020_buffers WHERE buff_mi = 5 AND "GEOID" IN ('
    + "'"
    + string_of_land_block_group_geoids
    + "')"
)  # Uses my function to bring in the shapefile containing the 2020 LAND block group centroids' 5-mile buffers to use that Sean Lawrence created which incorporate the Delaware River

costar_property_locations = db.get_geodataframe_from_query(
    "SELECT rentable_building_area, geom FROM analysis.costarproperties_region_plus_surrounding"
)  # Uses my function to bring in the shapefile containing the costar property locations

block_centroids_2020_geometries = db.get_geodataframe_from_query(
    'SELECT "GEOID20", geom FROM analysis.block_centroids_2020_with_2020_decennial_pop_and_hhs WHERE block_group20 IN ('
    + "'"
    + string_of_land_block_group_geoids
    + "')"
)  # Uses my function to bring in just the centroids of the 2020 LAND blocks

block_centroids_2020_with_2020_gq_hu = db.get_dataframe_from_query(
    'SELECT CONCAT(state,county,tract,block) AS "GEOID20", p5_001n+h1_001n AS gq_hu FROM _raw.tot_pops_and_hhs_2020_block WHERE block_group20 IN ('
    + "'"
    + string_of_land_block_group_geoids
    + "')"
)  # Uses my function to bring in each 2020 LAND block/centroid's total group quarters population and total housing units


costar_property_locations.insert(
    1, "comm_sqft_thou", costar_property_locations["rentable_building_area"] / 1000
)  # Adds the comm_sqft_thou column to costar_property_locations, and makes it the 2nd column

costar_property_locations_2mibuffers_overlay = gpd.overlay(
    costar_property_locations,
    buffers_2_mile,
    how="intersection",
)  # Gives each property location in costar_property_locations the GEOIDs of the 2020 LAND block group centroid 2-mile buffers they're in (this also produces multiple records per property location, since 1 centroid can be in numerous 2-mile buffers)

costar_property_locations_2mibuffers_overlay[
    "comm_sqft_thou"
] = costar_property_locations_2mibuffers_overlay["comm_sqft_thou"].fillna(
    0
)  # Makes the nulls in comm_sqft_thou 0

data_for_tot_comm_sqft_thou_2mi_column_step1 = (
    costar_property_locations_2mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({"comm_sqft_thou": "sum"})
    .rename(columns={"comm_sqft_thou": "tot_comm_sqft_thou_2mi"})
)  # For each 2-mile buffer 2020 GEOID (LAND block group ID/block group) that has at least 1 commercial property within the buffer, gets the total commercial square feet (in thousands), thereby creating the eventual tot_comm_sqft_thou_2mi column for the LAND block groups

data_for_tot_comm_sqft_thou_2mi_column_step2 = pd.DataFrame(
    {
        "GEOID": list(
            set(land_block_group_geoids)
            - set(list(data_for_tot_comm_sqft_thou_2mi_column_step1["GEOID"]))
        ),
        "tot_comm_sqft_thou_2mi": 0,
    }
)  # Creates a data frame to append to data_for_tot_comm_sqft_thou_2mi_column_step1 consisting of any LAND block group that has no commercial properties within 2 miles of their centroid

data_for_tot_comm_sqft_thou_2mi_column = pd.concat(
    [data_for_tot_comm_sqft_thou_2mi_column_step1, data_for_tot_comm_sqft_thou_2mi_column_step2]
)  # Creates data_for_tot_comm_sqft_thou_2mi_column which includes all LAND block groups


block_centroids_2020_with_2020_gq_hu = block_centroids_2020_geometries.merge(
    block_centroids_2020_with_2020_gq_hu, on=["GEOID20"], how="left"
)  # Makes it so each 2020 LAND block centroid has its total group quarters population plus total housing units

block_centroids_2020_with_2020_gq_hu_5mibuffers_overlay = gpd.overlay(
    block_centroids_2020_with_2020_gq_hu,
    buffers_5_mile,
    how="intersection",
)  # NOTE THAT THIS COMMAND TAKES THE LONGEST TIME TO RUN BY A NOTABLE AMOUNT. It gives each 2020 LAND block centroid in block_centroids_2020_with_2020_gq_hu the GEOIDs of the 2020 LAND block group centroid 5-mile buffers they're in (this also produces multiple records per 2010 block centroid, since 1 centroid can be in numerous 5-mile buffers)

data_for_tot_gq_hu_5mi_column = (
    block_centroids_2020_with_2020_gq_hu_5mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({"gq_hu": "sum"})
    .rename(columns={"gq_hu": "tot_gq_hu_5mi"})
)  # For each 5-mile buffer 2020 GEOID (LAND block group ID/block group), gets the total group quarters population and total housing units, thereby creating the eventual tot_gq_hu_5mi column for the LAND block groups

data_for_proximity_index_column = pd.merge(
    data_for_tot_comm_sqft_thou_2mi_column,
    data_for_tot_gq_hu_5mi_column,
    on=["GEOID"],
    how="left",
)  # Left joins data_for_tot_gq_hu_5mi_column to data_for_tot_comm_sqft_thou_2mi_column to essentially start getting the data for the eventual proximity_index column for the LAND block groups

data_for_proximity_index_column = data_for_proximity_index_column.rename(
    columns={"GEOID": "block_group20"}
)  # Renames GEOID to block_group20

data_for_proximity_index_column["proximity_index"] = (
    (
        data_for_proximity_index_column["tot_comm_sqft_thou_2mi"]
        * data_for_proximity_index_column["tot_gq_hu_5mi"]
    )
    * 2
) / (
    data_for_proximity_index_column["tot_comm_sqft_thou_2mi"]
    + data_for_proximity_index_column["tot_gq_hu_5mi"]
)  # Creates the actual eventual proximity_index column for the LAND block groups

data_for_proximity_index_column["proximity_index"] = data_for_proximity_index_column[
    "proximity_index"
].round(
    3
)  # Rounds proximity_index to the nearest 3 decimal places

data_for_proximity_index_column = data_for_proximity_index_column[
    ["block_group20", "proximity_index"]
]  # Just keeps the columns I want to keep and in the order I want to keep them in

null_proximity_index_records = db.get_dataframe_from_query(
    'SELECT "GEOID" AS block_group20, NULL AS proximity_index FROM analysis.block_groups_24co_2020 WHERE "ALAND" = 0'
)  # Creates the records which will have no proximity_index value in the eventual proximity_index_step1

proximity_index_step1 = pd.concat(
    [data_for_proximity_index_column, null_proximity_index_records]
)  # Creates the final proximity_index_step1

db.import_dataframe(
    proximity_index_step1,
    "analysis.proximity_index_step1",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed proximity_index_step1
