"""
This script edits the existing density and accessibility table
"""


import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


existing_density_and_accessibility_table = db.get_dataframe_from_query(
    "SELECT * FROM analysis.forecast_density_and_accessibility"
)  # Uses my function to bring in the existing density ad accessibility table

block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_dvrpc_2020"
)  # Uses my function to bring in the analysis.block_groups_dvrpc_2020 shapefile

tot_hus_2020_bg = db.get_dataframe_from_query(
    'SELECT CONCAT(state,county,tract,block_group) AS "GEOID", h1_001n AS housing_units_d20, FROM _raw.tot_pops_and_hhs_2020_bg'
)  # Uses my function to bring in the 2020 Decennial housing units by block group data

properties_with_comm_sqft_thou = db.get_geodataframe_from_query(
    'SELECT "GEOID10", comm_sqft_thou, geom FROM analysis.costar'
)  # Uses my function to bring in the shapefile containing the total commercial square feet (in thousands) for each property location

block_centroids_2020_with_2020_total_pop = db.get_geodataframe_from_query(
    'SELECT "GEOID20", "POP20", geom FROM analysis.block_centroids_2020_with_2020_decennial_pop_and_hhs'
)  # Uses my function to bring in the shapefile containing the total population for each 2010 block centroid

data_for_aland_acres_column = db.get_geodataframe_from_query(
    'SELECT "GEOID", aland_acres, geom FROM analysis.unprotected_land_area'
)  # Uses my function to bring in the shapefile containing data needed to make the eventual density column

block_group_centroid_buffers_dvrpc_2020_2_mile = db.get_geodataframe_from_query(
    'SELECT "GEOID", buff_mi, geom FROM analysis.block_group_centroid_buffers_dvrpc_2020 WHERE buff_mi = 2'
)  # Uses my function to bring in the shapefile containing the 2020 block group centroids' 2-mile buffers

block_group_centroid_buffers_dvrpc_2020_5_mile = db.get_geodataframe_from_query(
    'SELECT "GEOID", buff_mi, geom FROM analysis.block_group_centroid_buffers_dvrpc_2020 WHERE buff_mi = 5'
)  # Uses my function to bring in the shapefile containing the 2020 block group centroids' 5-mile buffers


block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    tot_hus_2020_bg, on=["GEOID"], how="left"
)  # Left joins tot_hus_2020_bg to block_groups_dvrpc_2020


properties_with_comm_sqft_thou_bgsdvrpc20_overlay = gpd.overlay(
    properties_with_comm_sqft_thou, block_groups_dvrpc_2020, how="intersection"
)  # Gives each property location in properties_with_comm_sqft_thou its 2020 block group GEOID

numerators_for_density_bones_calculations = (
    properties_with_comm_sqft_thou_bgsdvrpc20_overlay.groupby(["GEOID"], as_index=False)
    .agg(
        {
            "comm_sqft_thou": "sum",
            "housing_units_d20": "sum",
        }
    )
    .rename(
        columns={
            "comm_sqft_thou": "total_employment",
            "housing_units_d20": "total_housing_units",
        }
    )
)  # For each (2020) GEOID (block group ID/block group), gets the total commercial square feet (in thousands) and total number of housing units

numerators_for_density_bones_calculations["density_bones_numerator"] = (
    numerators_for_density_bones_calculations["total_employment"]
    + numerators_for_density_bones_calculations["total_housing_units"]
)  # Gets the numerators for the density_bones calculations by adding together total_employment and total_housing_units

numerators_for_density_bones_calculations = numerators_for_density_bones_calculations[
    ["GEOID", "density_bones_numerator"]
]  # Just keeps the columns I want to keep and in the order I want to keep them in

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    numerators_for_density_bones_calculations, on=["GEOID"], how="left"
)  # Left joins numerators_for_density_bones_calculations to block_groups_dvrpc_2020


data_for_aland_acres_column = (
    data_for_aland_acres_column.groupby(["GEOID"], as_index=False)
    .agg({"aland_acres": "sum"})
    .rename(columns={"aland_acres": "total_aland_acres"})
)  # For each GEOID (block group ID/block group), gets the total aland_acres value (also makes data_for_aland_acres_column non-spatial in the process)

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    data_for_aland_acres_column, on=["GEOID"], how="left"
)  # Left joins data_for_aland_acres_column to block_groups_dvrpc_2020


block_groups_dvrpc_2020["density_bones"] = (
    block_groups_dvrpc_2020["density_bones_numerator"]
    / block_groups_dvrpc_2020["total_aland_acres"]
)  # Divides the numerator for density_bones calculations column by total_aland_acres to get the eventual density_bones and accessibility_bones table's density_bones column


properties_with_comm_sqft_thou_2mibuffers_overlay = gpd.overlay(
    properties_with_comm_sqft_thou,
    block_group_centroid_buffers_dvrpc_2020_2_mile,
    how="intersection",
)  # Gives each property location in properties_with_comm_sqft_thou the GEOIDs of the 2020 block group centroid 2-mile buffers they're in (this also produces multiple records per property location, since 1 centroid can be in numerous 2-mile buffers)

data_for_tot_comm_sqft_thou_2mi_column = (
    properties_with_comm_sqft_thou_2mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({"comm_sqft_thou": "sum"})
    .rename(columns={"comm_sqft_thou": "tot_comm_sqft_thou_2mi"})
)  # For each 2-mile buffer 2020 GEOID (block group ID/block group), gets the total commercial square feet (in thousands), thereby creating the eventual tot_comm_sqft_thou_2mi column


block_centroids_2020_with_2020_total_pop_5mibuffers_overlay = gpd.overlay(
    block_centroids_2020_with_2020_total_pop,
    block_group_centroid_buffers_dvrpc_2020_5_mile,
    how="intersection",
)  # Gives each 2020 block centroid in block_centroids_2020_with_2020_total_pop the GEOIDs of the 2020 block group centroid 5-mile buffers they're in (this also produces multiple records per 2010 block centroid, since 1 centroid can be in numerous 5-mile buffers)

data_for_tot_pop_5mi_column = (
    block_centroids_2020_with_2020_total_pop_5mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({"POP20": "sum"})
    .rename(columns={"POP20": "tot_pop_5mi"})
)  # For each 5-mile buffer 2020 GEOID (block group ID/block group), gets the total population, thereby creating the eventual tot_pop_5mi column

data_for_accessibility_bones_columns = pd.merge(
    data_for_tot_comm_sqft_thou_2mi_column,
    data_for_tot_pop_5mi_column,
    on=["GEOID"],
    how="left",
)  # Left joins data_for_tot_pop_5mi_column to data_for_tot_comm_sqft_thou_2mi_column to essentially start getting the data for the eventual accessibility_bones columns


properties_with_comm_sqft_thou_5mibuffers_overlay = gpd.overlay(
    properties_with_comm_sqft_thou,
    block_group_centroid_buffers_dvrpc_2020_5_mile,
    how="intersection",
)  # Gives each property location in properties_with_comm_sqft_thou the GEOIDs of the 2020 block group centroid 5-mile buffers they're in (this also produces multiple records per property location, since 1 centroid can be in numerous 5-mile buffers)


data_for_accessibility_bones_columns["accessibility_bones"] = (
    (
        data_for_accessibility_bones_columns["tot_comm_sqft_thou_2mi"]
        * data_for_accessibility_bones_columns["tot_pop_5mi"]
    )
    * 2
) / (
    data_for_accessibility_bones_columns["tot_comm_sqft_thou_2mi"]
    + data_for_accessibility_bones_columns["tot_pop_5mi"]
)  # Creates the actual eventual accessibility_bones column

data_for_accessibility_bones_columns = data_for_accessibility_bones_columns[
    ["GEOID", "accessibility_bones"]
]  # Just keeps the columns I want to keep and in the order I want to keep them in

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    data_for_accessibility_bones_columns, on=["GEOID"], how="left"
)  # Left joins data_for_accessibility_bones_columns to block_groups_dvrpc_2020


density_bones_and_accessibility_bones = pd.DataFrame(
    block_groups_dvrpc_2020[["GEOID", "density_bones", "accessibility_bones"]]
).rename(
    columns={"GEOID": "block_group20"}
)  # Starts creating the density_bones and accessibility_bones table's by creating a new object that simultaneously keeps only the columns I want and with the names I want them from block_groups_dvrpc_2020 and in a non-spatial way

density_bones_and_accessibility_bones[
    ["density_bones", "accessibility_bones"]
] = density_bones_and_accessibility_bones[["density_bones", "accessibility_bones"]].round(
    2
)  # Rounds density_bones and accessibility_bones to the nearest 2 decimal places


density_and_accessibility_bones = pd.merge(
    existing_density_and_accessibility_table,
    density_bones_and_accessibility_bones,
    on=["block_group20"],
    how="left",
)  # Left joins density_bones_and_accessibility_bones back to the the existing density ad accessibility table to create the final results

db.import_dataframe(
    density_and_accessibility_bones,
    "analysis.density_and_accessibility_bones",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed density_and_accessibility_bones
