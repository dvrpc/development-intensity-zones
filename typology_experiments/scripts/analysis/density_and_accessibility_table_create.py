"""
This script creates a density and accessibility table to put into analysis based on user input. ALSO, EVEN THOUGH WE'RE NOT MAKING ANY lodes TABLES ETC ANYMORE, STILL KEEP THE SCRIPT WRITTEN IN THIS WAY, AT THE VERY LEAST FOR NOW
"""

employment_column_name = "combo_emp"  # USER DECLARES COLUMN NAME HERE (WOULD BE EITHER combo_emp OR lodes_emp). KEEP THIS SET AS combo_emp FROM NOW ON, AT LEAST FOR NOW

first_part_of_table_name = "forecast"  # USER DECLARES FIRST PART OF TABLE NAME HERE (WOULD BE EITHER forecast OR lodes). KEEP THIS SET AS forecast FROM NOW ON, AT LEAST FOR NOW


import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_dvrpc_2020"
)  # Uses my function to bring in the analysis.block_groups_dvrpc_2020 shapefile

tot_pops_and_hhs_2020_bg = db.get_dataframe_from_query(
    'SELECT CONCAT(state,county,tract,block_group) AS "GEOID", h1_002n AS t_hhs_d20 FROM _raw.tot_pops_and_hhs_2020_bg'
)  # Uses my function to bring in the 2020 Decennial households by block group data

block_centroids_2010_with_emp = db.get_geodataframe_from_query(
    'SELECT "GEOID10", '
    + employment_column_name
    + ", "
    + employment_column_name[:5]
    + "_2050_emp, geom FROM analysis.block_centroids_2010_with_emp"
)  # Uses my function to bring in the shapefile containing the total number of jobs, but forecast and not, for each 2010 block centroid. Also, note that "combo" and "lodes" are both 5 letters each

block_centroids_2010_with_2050_pop_and_hhs_change = db.get_geodataframe_from_query(
    'SELECT "GEOID10", households_2050, population_change, geom FROM analysis.block_centroids_2010_with_2050_pop_and_hhs_change'
)  # Uses my function to bring in each block's 2050 UrbanSim households

block_centroids_2020_with_2020_total_pop = db.get_geodataframe_from_query(
    'SELECT "GEOID20", "POP20", geom FROM analysis.block_centroids_2020_with_2020_decennial_pop_and_hhs'
)  # Uses my function to bring in the shapefile containing the total number of jobs for each 2010 block centroid

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
    tot_pops_and_hhs_2020_bg, on=["GEOID"], how="left"
)  # Left joins tot_pops_and_hhs_2020_bg to block_groups_dvrpc_2020


block_centroids_2010_with_2050_pop_and_hhs_change = pd.DataFrame(
    block_centroids_2010_with_2050_pop_and_hhs_change.drop(columns=["geom"])
)  # Makes block_centroids_2010_with_2050_pop_and_hhs_change non-spatial

block_centroids_2010_with_emp = block_centroids_2010_with_emp.merge(
    block_centroids_2010_with_2050_pop_and_hhs_change, on=["GEOID10"], how="left"
)  # Left joins block_centroids_2010_with_2050_pop_and_hhs_change to block_centroids_2010_with_emp so that each record in block_centroids_2010_with_emp has a households_2050 value


block_centroids_2010_with_emp_bgsdvrpc20_overlay = gpd.overlay(
    block_centroids_2010_with_emp, block_groups_dvrpc_2020, how="intersection"
)  # Gives each 2010 block centroid in block_centroids_2010_with_emp its 2020 block group GEOID

numerators_for_density_calculations = (
    block_centroids_2010_with_emp_bgsdvrpc20_overlay.groupby(["GEOID"], as_index=False)
    .agg(
        {
            employment_column_name: "sum",
            "t_hhs_d20": "sum",
            employment_column_name[:5] + "_2050_emp": "sum",
            "households_2050": "sum",
        }
    )
    .rename(
        columns={
            employment_column_name: "total_employment",
            "t_hhs_d20": "total_households",
            employment_column_name[:5] + "_2050_emp": "total_2050_employment",
            "households_2050": "total_2050_households",
        }
    )
)  # For each (2020) GEOID (block group ID/block group), gets the total number of jobs/employment, total number of households, total number of jobs/employment in 2050, and total number of households in 2050

numerators_for_density_calculations["density_numerator"] = (
    numerators_for_density_calculations["total_employment"]
    + numerators_for_density_calculations["total_households"]
)  # Gets the numerators for the density calculations by adding together total_employment and total_households

numerators_for_density_calculations["density_2050_numerator"] = (
    numerators_for_density_calculations["density_numerator"]
    + numerators_for_density_calculations["total_2050_employment"]
    + numerators_for_density_calculations["total_2050_households"]
)  # Gets the numerators for the 2050 density calculations by adding together density_numerator (which is total_employment + total_households), total_2050_employment and total_2050_households

numerators_for_density_calculations = numerators_for_density_calculations[
    ["GEOID", "density_numerator", "density_2050_numerator"]
]  # Just keeps the columns I want to keep and in the order I want to keep them in

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    numerators_for_density_calculations, on=["GEOID"], how="left"
)  # Left joins numerators_for_density_calculations to block_groups_dvrpc_2020


data_for_aland_acres_column = (
    data_for_aland_acres_column.groupby(["GEOID"], as_index=False)
    .agg({"aland_acres": "sum"})
    .rename(columns={"aland_acres": "total_aland_acres"})
)  # For each GEOID (block group ID/block group), gets the total aland_acres value (also makes data_for_aland_acres_column non-spatial in the process)

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    data_for_aland_acres_column, on=["GEOID"], how="left"
)  # Left joins data_for_aland_acres_column to block_groups_dvrpc_2020


block_groups_dvrpc_2020["density"] = (
    block_groups_dvrpc_2020["density_numerator"] / block_groups_dvrpc_2020["total_aland_acres"]
)  # Divides the numerator for density calculations column by total_aland_acres to get the eventual density and accessibility table's density column

block_groups_dvrpc_2020["density_2050"] = (
    block_groups_dvrpc_2020["density_2050_numerator"] / block_groups_dvrpc_2020["total_aland_acres"]
)  # Divides the numerator for the 2050 density calculations column by total_aland_acres to get the eventual density and accessibility table's density_2050 column


block_centroids_2010_with_emp_2mibuffers_overlay = gpd.overlay(
    block_centroids_2010_with_emp,
    block_group_centroid_buffers_dvrpc_2020_2_mile,
    how="intersection",
)  # Gives each 2010 block centroid in block_centroids_2010_with_emp the GEOIDs of the 2020 block group centroid 2-mile buffers they're in (this also produces multiple records per 2010 block centroid, since 1 centroid can be in numerous 2-mile buffers)

data_for_tot_emp_2mi_column = (
    block_centroids_2010_with_emp_2mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({employment_column_name: "sum"})
    .rename(columns={employment_column_name: "tot_emp_2mi"})
)  # For each 2-mile buffer 2020 GEOID (block group ID/block group), gets the total number of jobs/employment, thereby creating the eventual tot_emp_2mi column


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

data_for_accessibility_columns = pd.merge(
    data_for_tot_emp_2mi_column,
    data_for_tot_pop_5mi_column,
    on=["GEOID"],
    how="left",
)  # Left joins data_for_tot_pop_5mi_column to data_for_tot_emp_2mi_column to essentially start getting the data for the eventual accessibility columns

data_for_pop_change_5mi_column = (
    block_centroids_2020_with_2020_total_pop_5mibuffers_overlay.groupby(["GEOID"], as_index=False)
    .agg({"population_change": "sum"})
    .rename(columns={"population_change": "pop_change_5mi"})
)  # For each 5-mile buffer 2020 GEOID (block group ID/block group), gets the total population change between 2050 and 2020, thereby creating the eventual pop_change_5mi column

data_for_accessibility_columns = pd.merge(
    data_for_accessibility_columns,
    data_for_pop_change_5mi_column,
    on=["GEOID"],
    how="left",
)  # Left joins data_for_pop_change_5mi_column to data_for_accessibility_columns to finish getting the data for the eventual accessibility columns

data_for_accessibility_columns["accessibility"] = (
    (data_for_accessibility_columns["tot_emp_2mi"] * data_for_accessibility_columns["tot_pop_5mi"])
    * 2
) / (
    data_for_accessibility_columns["tot_emp_2mi"] + data_for_accessibility_columns["tot_pop_5mi"]
)  # Creates the actual eventual accessibility column

data_for_accessibility_columns["accessibility_2050"] = (
    (
        data_for_accessibility_columns["tot_emp_2mi"]
        * (
            data_for_accessibility_columns["tot_pop_5mi"]
            + data_for_accessibility_columns["pop_change_5mi"]
        )
    )
    * 2
) / (
    data_for_accessibility_columns["tot_emp_2mi"]
    + (
        data_for_accessibility_columns["tot_pop_5mi"]
        + data_for_accessibility_columns["pop_change_5mi"]
    )
)  # Creates the actual eventual 2050 accessibility column

data_for_accessibility_columns = data_for_accessibility_columns[
    ["GEOID", "accessibility", "accessibility_2050"]
]  # Just keeps the columns I want to keep and in the order I want to keep them in

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    data_for_accessibility_columns, on=["GEOID"], how="left"
)  # Left joins data_for_accessibility_columns to block_groups_dvrpc_2020


density_and_accessibility = pd.DataFrame(
    block_groups_dvrpc_2020[
        ["GEOID", "density", "accessibility", "density_2050", "accessibility_2050"]
    ]
).rename(
    columns={"GEOID": "block_group20"}
)  # Starts creating the density and accessibility table's by creating a new object that simultaneously keeps only the columns I want and with the names I want them from block_groups_dvrpc_2020 and in a non-spatial way

density_and_accessibility[
    ["density", "accessibility", "density_2050", "accessibility_2050"]
] = density_and_accessibility[
    ["density", "accessibility", "density_2050", "accessibility_2050"]
].round(
    2
)  # Rounds density, accessibility, density_2050, and accessibility_2050 to the nearest 2 decimal places

db.import_dataframe(
    density_and_accessibility,
    "analysis." + first_part_of_table_name + "_density_and_accessibility",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed density and accessibility table
