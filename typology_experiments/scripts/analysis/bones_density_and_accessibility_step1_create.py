"""
This script creates analysis.bones_density_and_accessibility_step1. ALSO, REMEMBER TO FIRST CASCADE DELETE/DROP CASCADE analysis.bones_density_and_accessibility_step1 BEFORE RUNNING THIS SCRIPT, AND THEN AFTER FINISHING RUNNING THIS SCRIPT, RERUN THE SCRIPTS THAT CREATE THE VIEWS THAT DEPEND ON analysis.bones_density_and_accessibility_step1. ALSO, WHAT DISTINGUISHES analysis.bones_density_and_accessibility_step1 FROM analysis.bones_density_and_accessibility IS THAT analysis.bones_density_and_accessibility_step1 IS USED TO CREATE THE BUFFERS THAT INCORPORATE THE DELAWARE RIVER, VERSUS analysis.bones_density_and_accessibility WHICH ALREADY INCORPORATES THE DELAWARE RIVER
"""


import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


block_groups_24co_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_24co_2020"
)  # Uses my function to bring in the analysis.block_groups_24co_2020 shapefile

tot_hus_2020_bg = db.get_dataframe_from_query(
    'SELECT CONCAT(state,county,tract,block_group) AS "GEOID", p5_001n+h1_001n AS housing_units_d20 FROM _raw.tot_pops_and_hhs_2020_bg'
)  # Uses my function to bring in the 2020 Decennial housing units by block group data (also, note how we're adding the total group quarters population to the total housing units here)

costarproperties_rentable_area_bg = db.get_dataframe_from_query(
    'SELECT "GEOID", commercial_sqft AS comm_sqft_thou FROM analysis.costarproperties_rentable_area_bg'
)  # Uses my function to bring in the shapefile containing the total commercial square feet (IN THOUSANDS) for each property location

data_for_aland_acres_column = db.get_geodataframe_from_query(
    'SELECT "GEOID", aland_acres, geom FROM analysis.unprotected_land_area'
)  # Uses my function to bring in the shapefile containing data needed to make the eventual density column

costar_property_locations = db.get_geodataframe_from_query(
    "SELECT rentable_building_area, geom FROM analysis.costarproperties_region_plus_surrounding"
)  # Uses my function to bring in the shapefile containing the costar property locations

block_group_centroid_buffers_24co_2020_2_mile = db.get_geodataframe_from_query(
    'SELECT "GEOID", buff_mi, geom FROM analysis.block_group_centroid_buffers_24co_2020 WHERE buff_mi = 2'
)  # Uses my function to bring in the shapefile containing the 2020 block group centroids' 2-mile buffers

block_group_centroid_buffers_24co_2020_5_mile = db.get_geodataframe_from_query(
    'SELECT "GEOID", buff_mi, geom FROM analysis.block_group_centroid_buffers_24co_2020 WHERE buff_mi = 5'
)  # Uses my function to bring in the shapefile containing the 2020 block group centroids' 5-mile buffers


block_centroids_2020_geometries = db.get_geodataframe_from_query(
    'SELECT "GEOID20", geom FROM analysis.block_centroids_2020_with_2020_decennial_pop_and_hhs'
)  # Uses my function to bring in just the centroids of the 2020 blocks

block_centroids_2020_with_2020_gq_hu = db.get_dataframe_from_query(
    'SELECT CONCAT(state,county,tract,block) AS "GEOID20", p5_001n+h1_001n AS gq_hu FROM _raw.tot_pops_and_hhs_2020_block'
)  # Uses my function to bring in each 2020 block/centroid's total group quarters population and total housing units

block_centroids_2020_with_2020_gq_hu = block_centroids_2020_geometries.merge(
    block_centroids_2020_with_2020_gq_hu, on=["GEOID20"], how="left"
)  # Makes it so each 2020 block centroid has its total group quarters population plus total housing units


block_groups_24co_2020 = block_groups_24co_2020.merge(
    tot_hus_2020_bg, on=["GEOID"], how="left"
)  # Left joins tot_hus_2020_bg to block_groups_24co_2020

block_groups_24co_2020 = block_groups_24co_2020.merge(
    costarproperties_rentable_area_bg, on=["GEOID"], how="left"
)  # Left joins costarproperties_rentable_area_bg to block_groups_24co_2020 as well


numerators_for_density_bones_calculations = (
    block_groups_24co_2020.groupby(["GEOID"], as_index=False)
    .agg(
        {
            "comm_sqft_thou": "sum",
            "housing_units_d20": "sum",
        }
    )
    .rename(
        columns={
            "comm_sqft_thou": "total_comm_sqft_thou",
            "housing_units_d20": "total_housing_units",
        }
    )
)  # For each (2020) GEOID (block group ID/block group), gets the total commercial square feet (in thousands) and total number of housing units

numerators_for_density_bones_calculations["density_bones_numerator"] = (
    numerators_for_density_bones_calculations["total_comm_sqft_thou"]
    + numerators_for_density_bones_calculations["total_housing_units"]
)  # Gets the numerators for the density_bones calculations by adding together total_comm_sqft_thou and total_housing_units

numerators_for_density_bones_calculations = numerators_for_density_bones_calculations[
    ["GEOID", "density_bones_numerator"]
]  # Just keeps the columns I want to keep and in the order I want to keep them in

block_groups_24co_2020 = block_groups_24co_2020.merge(
    numerators_for_density_bones_calculations, on=["GEOID"], how="left"
)  # Left joins numerators_for_density_bones_calculations to block_groups_24co_2020


data_for_aland_acres_column = (
    data_for_aland_acres_column.groupby(["GEOID"], as_index=False)
    .agg({"aland_acres": "sum"})
    .rename(columns={"aland_acres": "total_aland_acres"})
)  # For each GEOID (block group ID/block group), gets the total aland_acres value (also makes data_for_aland_acres_column non-spatial in the process)

block_groups_24co_2020 = block_groups_24co_2020.merge(
    data_for_aland_acres_column, on=["GEOID"], how="left"
)  # Left joins data_for_aland_acres_column to block_groups_24co_2020


block_groups_24co_2020["density_bones"] = (
    block_groups_24co_2020["density_bones_numerator"] / block_groups_24co_2020["total_aland_acres"]
)  # Divides the numerator for density_bones calculations column by total_aland_acres to get the eventual density_bones and accessibility_bones table's density_bones column


costar_property_locations.insert(
    1, "comm_sqft_thou", costar_property_locations["rentable_building_area"] / 1000
)  # Adds the comm_sqft_thou column to costar_property_locations, and makes it the 2nd column

costar_property_locations_2mibuffers_overlay = gpd.overlay(
    costar_property_locations,
    block_group_centroid_buffers_24co_2020_2_mile,
    how="intersection",
)  # Gives each property location in costar_property_locations the GEOIDs of the 2020 block group centroid 2-mile buffers they're in (this also produces multiple records per property location, since 1 centroid can be in numerous 2-mile buffers)

data_for_tot_comm_sqft_thou_2mi_column = (
    costar_property_locations_2mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({"comm_sqft_thou": "sum"})
    .rename(columns={"comm_sqft_thou": "tot_comm_sqft_thou_2mi"})
)  # For each 2-mile buffer 2020 GEOID (block group ID/block group), gets the total commercial square feet (in thousands), thereby creating the eventual tot_comm_sqft_thou_2mi column


block_centroids_2020_with_2020_gq_hu_5mibuffers_overlay = gpd.overlay(
    block_centroids_2020_with_2020_gq_hu,
    block_group_centroid_buffers_24co_2020_5_mile,
    how="intersection",
)  # Gives each 2020 block centroid in block_centroids_2020_with_2020_gq_hu the GEOIDs of the 2020 block group centroid 5-mile buffers they're in (this also produces multiple records per 2010 block centroid, since 1 centroid can be in numerous 5-mile buffers)

data_for_tot_gq_hu_5mi_column = (
    block_centroids_2020_with_2020_gq_hu_5mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({"gq_hu": "sum"})
    .rename(columns={"gq_hu": "tot_gq_hu_5mi"})
)  # For each 5-mile buffer 2020 GEOID (block group ID/block group), gets the total group quarters population and total housing units, thereby creating the eventual tot_gq_hu_5mi column

data_for_accessibility_bones_columns = pd.merge(
    data_for_tot_comm_sqft_thou_2mi_column,
    data_for_tot_gq_hu_5mi_column,
    on=["GEOID"],
    how="left",
)  # Left joins data_for_tot_gq_hu_5mi_column to data_for_tot_comm_sqft_thou_2mi_column to essentially start getting the data for the eventual accessibility_bones columns


data_for_accessibility_bones_columns["accessibility_bones"] = (
    (
        data_for_accessibility_bones_columns["tot_comm_sqft_thou_2mi"]
        * data_for_accessibility_bones_columns["tot_gq_hu_5mi"]
    )
    * 2
) / (
    data_for_accessibility_bones_columns["tot_comm_sqft_thou_2mi"]
    + data_for_accessibility_bones_columns["tot_gq_hu_5mi"]
)  # Creates the actual eventual accessibility_bones column

data_for_accessibility_bones_columns = data_for_accessibility_bones_columns[
    ["GEOID", "accessibility_bones"]
]  # Just keeps the columns I want to keep and in the order I want to keep them in

block_groups_24co_2020 = block_groups_24co_2020.merge(
    data_for_accessibility_bones_columns, on=["GEOID"], how="left"
)  # Left joins data_for_accessibility_bones_columns to block_groups_24co_2020


bones_density_and_accessibility_step1 = pd.DataFrame(
    block_groups_24co_2020[["GEOID", "density_bones", "accessibility_bones"]]
).rename(
    columns={"GEOID": "block_group20"}
)  # Starts creating the eventual analysis.bones_density_and_accessibility_step1 by creating a new object that simultaneously keeps only the columns I want and with the names I want them from block_groups_24co_2020 and in a non-spatial way

bones_density_and_accessibility_step1[
    ["density_bones", "accessibility_bones"]
] = bones_density_and_accessibility_step1[["density_bones", "accessibility_bones"]].round(
    2
)  # Rounds density_bones and accessibility_bones to the nearest 2 decimal places

db.import_dataframe(
    bones_density_and_accessibility_step1,
    "analysis.bones_density_and_accessibility_step1",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed bones_density_and_accessibility_step1
