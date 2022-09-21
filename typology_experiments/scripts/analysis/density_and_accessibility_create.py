"""
This script creates analysis.density_and_accessibility
"""

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

block_centroids_2010_with_2018_total_jobs = db.get_geodataframe_from_query(
    'SELECT "GEOID10", c000 AS ttl_jbs_18, geom FROM analysis.block_centroids_2010_with_2018_lodes_emp'
)  # Uses my function to bring in the shapefile containing the total number of jobs for each 2010 block centroid

block_centroids_2020_with_2020_total_pop = db.get_geodataframe_from_query(
    'SELECT "GEOID20", t_pop_d20 AS ttl_pop_20, geom FROM analysis.block_centroids_2020_with_2020_decennial_pop'
)  # Uses my function to bring in the shapefile containing the total number of jobs for each 2010 block centroid

data_for_aland_acres_and_non_pos_water_acres_columns = db.get_geodataframe_from_query(
    'SELECT "GEOID", aland_acres, non_pos_water_acres, geom FROM analysis.unprotected_land_area'
)  # Uses my function to bring in the shapefile containing the data for the eventual aland_acres and non_pos_water_acres columns

block_group_centroid_buffers_dvrpc_2020_2_mile = db.get_geodataframe_from_query(
    'SELECT "GEOID", buff_mi, geom FROM analysis.block_group_centroid_buffers_dvrpc_2020 WHERE buff_mi = 2'
)  # Uses my function to bring in the shapefile containing the 2020 block group centroids' 2-mile buffers

block_group_centroid_buffers_dvrpc_2020_5_mile = db.get_geodataframe_from_query(
    'SELECT "GEOID", buff_mi, geom FROM analysis.block_group_centroid_buffers_dvrpc_2020 WHERE buff_mi = 5'
)  # Uses my function to bring in the shapefile containing the 2020 block group centroids' 5-mile buffers


block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    tot_pops_and_hhs_2020_bg, on=["GEOID"], how="left"
)  # Left joins tot_pops_and_hhs_2020_bg to block_groups_dvrpc_2020


block_groups_dvrpc_2020 = gpd.sjoin(
    block_groups_dvrpc_2020,
    block_centroids_2010_with_2018_total_jobs,
    how="left",
    predicate="within",
)  # Left one-to-one spatial joins the block_centroids_2010_with_2018_total_jobs points to the block_groups_dvrpc_2020 polygons

numerators_for_density_calculations = (
    block_groups_dvrpc_2020.groupby(["GEOID"], as_index=False)
    .agg({"ttl_jbs_18": "sum", "t_hhs_d20": "sum"})
    .rename(columns={"ttl_jbs_18": "total_employment", "t_hhs_d20": "total_households"})
)  # For each GEOID (block group ID/block group), gets the total number of jobs/employment and total number of households

numerators_for_density_calculations["density_numerator"] = (
    numerators_for_density_calculations["total_employment"]
    + numerators_for_density_calculations["total_households"]
)  # Gets the numerators for the density calculations by adding together total_employment and total_households

numerators_for_density_calculations = numerators_for_density_calculations[
    ["GEOID", "density_numerator"]
]  # Just keeps the columns I want to keep

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    numerators_for_density_calculations, on=["GEOID"], how="left"
)  # Left joins numerators_for_density_calculations to block_groups_dvrpc_2020


data_for_aland_acres_and_non_pos_water_acres_columns = (
    data_for_aland_acres_and_non_pos_water_acres_columns.groupby(["GEOID"], as_index=False)
    .agg({"aland_acres": "sum", "non_pos_water_acres": "sum"})
    .rename(
        columns={
            "aland_acres": "total_aland_acres",
            "non_pos_water_acres": "total_non_pos_water_acres",
        }
    )
)  # For each GEOID (block group ID/block group), gets the total aland_acres and total non_pos_water_acres values (also makes data_for_aland_acres_and_non_pos_water_acres_columns non-spatial in the process)

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    data_for_aland_acres_and_non_pos_water_acres_columns, on=["GEOID"], how="left"
)  # Left joins data_for_aland_acres_and_non_pos_water_acres_columns to block_groups_dvrpc_2020


block_groups_dvrpc_2020["density"] = (
    block_groups_dvrpc_2020["density_numerator"] / block_groups_dvrpc_2020["total_aland_acres"]
)  # Divides the numerator for density calculations column by total_aland_acres to get the eventual analysis.density_and_accessibility's density column

block_groups_dvrpc_2020["alt_density"] = (
    block_groups_dvrpc_2020["density_numerator"]
    / block_groups_dvrpc_2020["total_non_pos_water_acres"]
)  # Divides the numerator for density calculations column by total_non_pos_water_acres to get the eventual analysis.density_and_accessibility's alt_density column


block_group_centroid_buffers_dvrpc_2020_2_mile = gpd.sjoin(
    block_group_centroid_buffers_dvrpc_2020_2_mile,
    block_centroids_2010_with_2018_total_jobs,
    how="left",
    predicate="intersects",
)  # Left one-to-many spatial joins the block_centroids_2010_with_2018_total_jobs points to the block_group_centroid_buffers_dvrpc_2020_2_mile polygons

block_group_centroid_buffers_dvrpc_2020_2_mile = block_group_centroid_buffers_dvrpc_2020_2_mile.sort_values(
    "GEOID10"
)  # Sorts/orders block_group_centroid_buffers_dvrpc_2020_2_mile by GEOID10, essentially just to make sure the left one-to-many spatial join worked properly

data_for_tot_emp_2mi_column = (
    block_group_centroid_buffers_dvrpc_2020_2_mile.groupby(["GEOID"], as_index=False)
    .agg({"ttl_jbs_18": "sum"})
    .rename(columns={"ttl_jbs_18": "tot_emp_2mi"})
)  # For each GEOID (block group ID/block group), gets the total number of jobs/employment, thereby creating the eventual tot_emp_2mi column

block_group_centroid_buffers_dvrpc_2020_5_mile = gpd.sjoin(
    block_group_centroid_buffers_dvrpc_2020_5_mile,
    block_centroids_2020_with_2020_total_pop,
    how="left",
    predicate="intersects",
)  # Left one-to-many spatial joins the block_centroids_2020_with_2020_total_pop points to the block_group_centroid_buffers_dvrpc_2020_5_mile polygons

block_group_centroid_buffers_dvrpc_2020_5_mile = block_group_centroid_buffers_dvrpc_2020_5_mile.sort_values(
    "GEOID20"
)  # Sorts/orders block_group_centroid_buffers_dvrpc_2020_5_mile by GEOID20, essentially just to make sure the left one-to-many spatial join worked properly

data_for_tot_pop_5mi_column = (
    block_group_centroid_buffers_dvrpc_2020_5_mile.groupby(["GEOID"], as_index=False)
    .agg({"ttl_pop_20": "sum"})
    .rename(columns={"ttl_pop_20": "tot_pop_5mi"})
)  # For each GEOID (block group ID/block group), gets the total population, thereby creating the eventual tot_pop_5mi column

data_for_accessibility_column = pd.merge(
    data_for_tot_emp_2mi_column, data_for_tot_pop_5mi_column, on=["GEOID"], how="left"
)  # Left joins data_for_tot_pop_5mi_column to data_for_tot_emp_2mi_column to get the data for the eventual accessibility column

data_for_accessibility_column["accessibility"] = (
    (data_for_accessibility_column["tot_emp_2mi"] * data_for_accessibility_column["tot_pop_5mi"])
    * 2
) / (
    data_for_accessibility_column["tot_emp_2mi"] + data_for_accessibility_column["tot_pop_5mi"]
)  # Creates the actual eventual accessibility column

data_for_accessibility_column = data_for_accessibility_column[
    ["GEOID", "accessibility"]
]  # Just keeps the columns I want to keep

block_groups_dvrpc_2020 = block_groups_dvrpc_2020.merge(
    data_for_accessibility_column, on=["GEOID"], how="left"
)  # Left joins data_for_accessibility_column to block_groups_dvrpc_2020


density_and_accessibility = pd.DataFrame(
    block_groups_dvrpc_2020[["GEOID", "density", "alt_density", "accessibility"]]
).rename(
    columns={"GEOID": "block_group20"}
)  # Starts creating analysis.density_and_accessibility by creating a new object that simultaneously keeps only the columns I want and with the names I want them from block_groups_dvrpc_2020 and in a non-spatial way

density_and_accessibility[["density", "alt_density", "accessibility"]] = density_and_accessibility[
    ["density", "alt_density", "accessibility"]
].round(
    2
)  # Rounds density, alt_density, and accessibility to the nearest 2 decimal places

db.import_dataframe(
    density_and_accessibility,
    "analysis.density_and_accessibility",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed table as analysis.density_and_accessibility
