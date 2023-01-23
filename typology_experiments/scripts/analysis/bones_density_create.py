"""
This script creates analysis.bones_density. ALSO, REMEMBER TO FIRST CASCADE DELETE/DROP CASCADE analysis.bones_density BEFORE RUNNING THIS SCRIPT, THEN RERUN ANY SCRIPTS THAT CREATE ANY VIEWS WHICH DEPEND ON analysis.bones_density
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
)  # Divides the numerator for density_bones calculations column by total_aland_acres to get the eventual analysis.bones_density's density_bones column


bones_density = pd.DataFrame(block_groups_24co_2020[["GEOID", "density_bones"]]).rename(
    columns={"GEOID": "block_group20"}
)  # Starts creating the eventual analysis.bones_density by creating a new object that simultaneously keeps only the columns I want and with the names I want them from block_groups_24co_2020 and in a non-spatial way

bones_density[["density_bones"]] = bones_density[["density_bones"]].round(
    2
)  # Rounds density_bones to the nearest 2 decimal places


db.import_dataframe(
    bones_density,
    "analysis.bones_density",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed bones_density
