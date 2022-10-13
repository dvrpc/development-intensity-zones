"""
This script creates analysis.block_centroids_2010_with_emp
"""

import pandas as pd

import geopandas as gpd

import numpy as np


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


blocks_all4states_2010 = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.tl_2010_10_tabblock10 UNION SELECT * FROM _raw.tl_2010_24_tabblock10 UNION SELECT * FROM _raw.tl_2010_34_tabblock10 UNION SELECT * FROM _raw.tl_2010_42_tabblock10"
)  # Uses my function to bring in the 2010 Delaware, Maryland, New Jersey and Pennsylvania blocks shapefiles, and combines them into 1 big one

blocks_all4states_2010_centroids = (
    blocks_all4states_2010.copy()
)  # Begins the process of getting all the blocks' centroids

blocks_all4states_2010_centroids["geom"] = blocks_all4states_2010_centroids[
    "geom"
].centroid  # Finishes the process of getting all the blocks' centroids


total_jobs_from_lodes = db.get_dataframe_from_query(
    'SELECT w_geocode, c000 AS lodes_emp FROM _raw."de_wac_S000_JT00_2018" UNION SELECT w_geocode, c000 AS lodes_emp FROM _raw."md_wac_S000_JT00_2018" UNION SELECT w_geocode, c000 AS lodes_emp FROM _raw."nj_wac_S000_JT00_2018" UNION SELECT w_geocode, c000 AS lodes_emp FROM _raw."pa_wac_S000_JT00_2018"'
)  # Uses my function to bring in just the columns I want from the 2018 Delaware, Maryland, New Jersey and Pennsylvania LODES tables, and combines them into 1 big table

total_jobs_from_lodes["GEOID10"] = total_jobs_from_lodes["w_geocode"].apply(
    str
)  # Makes a string column of w_geocode to get each block's GEOID10 value

block_centroids_2010_with_emp = blocks_all4states_2010_centroids.merge(
    total_jobs_from_lodes[["GEOID10", "lodes_emp"]], on=["GEOID10"], how="left"
)  # Left joins just the columns I want from total_jobs_from_lodes to blocks_all4states_2010_centroids to create block_centroids_2010_with_emp


total_jobs_from_dvrpc_emp_forecast = db.get_dataframe_from_query(
    "SELECT block_id, total_jobs AS forecast_2020_emp FROM _raw.dvrpc_forecast_2020_emp_block10"
)  # Uses my function to bring in the DVRPC 2020 employment forecast by 2010 block data

total_jobs_from_dvrpc_emp_forecast["GEOID10"] = total_jobs_from_dvrpc_emp_forecast[
    "block_id"
].apply(
    str
)  # Makes a string column of block_id to get each block's GEOID10 value

block_centroids_2010_with_emp = block_centroids_2010_with_emp.merge(
    total_jobs_from_dvrpc_emp_forecast[["GEOID10", "forecast_2020_emp"]], on=["GEOID10"], how="left"
)  # Left joins just the columns I want from total_jobs_from_dvrpc_emp_forecast to block_centroids_2010_with_emp so that it now has both lodes_emp and forecast_2020_emp


block_centroids_2010_with_emp["county_id_5dig"] = (
    block_centroids_2010_with_emp["STATEFP10"] + block_centroids_2010_with_emp["COUNTYFP10"]
)  # Makes a new column getting each block (centroid)'s county ID

dvrpc_county_id_5dig_values = [
    s for l in db.query("SELECT county_id_5dig FROM _resources.county_key") for s in l
]  # Uses my function to get a list of the DVRPC county census IDs

block_centroids_2010_with_emp["combo_emp"] = np.where(
    block_centroids_2010_with_emp["county_id_5dig"].isin(dvrpc_county_id_5dig_values),
    block_centroids_2010_with_emp["forecast_2020_emp"],
    block_centroids_2010_with_emp["lodes_emp"],
)  # Creates combo_emp by saying if the block centroid is in a DVRPC county, use the forecast_2020_emp value, otherwise use the lodes_emp value


dvrpc_2050_emp_forecast_by_2010_block = db.get_dataframe_from_query(
    'SELECT block_id AS "GEOID10", total_jobs AS forecast_2050_emp FROM _raw.urbansim_2050_by_block_2010'
)  # Uses my function to bring in the DVRPC 2050 employment forecast by 2010 block data

dvrpc_2050_emp_forecast_by_2010_block["GEOID10"] = dvrpc_2050_emp_forecast_by_2010_block[
    "GEOID10"
].astype(
    str
)  # Makes GEOID10 string so it can be left joined with block_centroids_2010_with_emp

block_centroids_2010_with_emp = block_centroids_2010_with_emp.merge(
    dvrpc_2050_emp_forecast_by_2010_block, on=["GEOID10"], how="left"
)  # Left joins dvrpc_2050_emp_forecast_by_2010_block to block_centroids_2010_with_emp

block_centroids_2010_with_emp["combo_2050_emp"] = np.where(
    block_centroids_2010_with_emp["county_id_5dig"].isin(dvrpc_county_id_5dig_values),
    block_centroids_2010_with_emp["forecast_2050_emp"],
    block_centroids_2010_with_emp["lodes_emp"],
)  # Creates combo_2050_emp by saying if the block centroid is in a DVRPC county, use the forecast_2050_emp value, otherwise use the lodes_emp value


block_centroids_2010_with_emp = block_centroids_2010_with_emp[
    [
        "STATEFP10",
        "COUNTYFP10",
        "county_id_5dig",
        "TRACTCE10",
        "BLOCKCE10",
        "GEOID10",
        "NAME10",
        "MTFCC10",
        "UR10",
        "UACE10",
        "UATYP10",
        "FUNCSTAT10",
        "ALAND10",
        "AWATER10",
        "INTPTLAT10",
        "INTPTLON10",
        "lodes_emp",
        "forecast_2020_emp",
        "forecast_2050_emp",
        "combo_emp",
        "combo_2050_emp",
        "geom",
    ]
]  # Simultaneously keeps only the columns I want and reorders them. AND LEAVE combo_emp NAMED AS THAT (INSTEAD OF CHANGING ITS NAME TO "combo_2020_emp"), AT LEAST FOR NOW

db.import_geodataframe(
    block_centroids_2010_with_emp,
    "block_centroids_2010_with_emp",
    schema="analysis",
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
