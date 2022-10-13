"""
This script creates analysis.block_centroids_2010_with_2050_pop_and_hhs_change
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


dvrpc_2050_emp_forecast_by_2010_block = db.get_dataframe_from_query(
    'SELECT block_id AS "GEOID10", total_jobs AS forecast_2050_emp FROM _raw.urbansim_2050_by_block_2010'
)  # Uses my function to bring in the DVRPC 2050 employment forecast by 2010 block data

dvrpc_2050_emp_forecast_by_2010_block["GEOID10"] = dvrpc_2050_emp_forecast_by_2010_block[
    "GEOID10"
].astype(
    str
)  # Makes GEOID10 string so it can be left joined with block_centroids_2010


block_centroids_2010 = db.get_geodataframe_from_query(
    'SELECT "STATEFP10", "COUNTYFP10", county_id_5dig, "TRACTCE10", "BLOCKCE10", "GEOID10", "NAME10", "MTFCC10", "UR10", "UACE10", "UATYP10", "FUNCSTAT10", "ALAND10", "AWATER10", "INTPTLAT10", "INTPTLON10", geom FROM analysis.block_centroids_2010_with_emp'
)  # Uses my function to bring in the 2010 block centroids


total_population_and_households_2020 = db.get_dataframe_from_query(
    'SELECT block_id AS "GEOID10", population_gq AS population_2020, total_households AS households_2020 FROM _raw.urbansim_2020_by_block_2010'
)  # Uses my function to bring in just the columns I want from _raw.urbansim_2020_by_block_2010

total_population_and_households_2020["GEOID10"] = total_population_and_households_2020[
    "GEOID10"
].astype(
    str
)  # Makes GEOID10 string


total_population_and_households_2050 = db.get_dataframe_from_query(
    'SELECT block_id AS "GEOID10", population_gq AS population_2050, total_households AS households_2050 FROM _raw.urbansim_2050_by_block_2010'
)  # This and the next command repeat the process, but for 2050

total_population_and_households_2050["GEOID10"] = total_population_and_households_2050[
    "GEOID10"
].astype(str)


total_population_and_households_2020_and_2050 = pd.merge(
    total_population_and_households_2020,
    total_population_and_households_2050,
    on=["GEOID10"],
    how="left",
)  # Left joins total_population_and_households_2050 to total_population_and_households_2020

total_population_and_households_2020_and_2050["population_change"] = (
    total_population_and_households_2020_and_2050["population_2050"]
    - total_population_and_households_2020_and_2050["population_2020"]
)  # Creates population_change

total_population_and_households_2020_and_2050["households_change"] = (
    total_population_and_households_2020_and_2050["households_2050"]
    - total_population_and_households_2020_and_2050["households_2020"]
)  # Creates households_change


block_centroids_2010_with_2050_pop_and_hhs_change = block_centroids_2010.merge(
    total_population_and_households_2020_and_2050, on=["GEOID10"], how="left"
)  # Left joins total_population_and_households_2020_and_2050 to block_centroids_2010 to create block_centroids_2010_with_2050_pop_and_hhs_change

block_centroids_2010_with_2050_pop_and_hhs_change = (
    block_centroids_2010_with_2050_pop_and_hhs_change[
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
            "population_2050",
            "population_2020",
            "population_change",
            "households_2050",
            "households_2020",
            "households_change",
            "geom",
        ]
    ]
)  # Simultaneously keeps only the columns I want and reorders them

db.import_geodataframe(
    block_centroids_2010_with_2050_pop_and_hhs_change,
    "block_centroids_2010_with_2050_pop_and_hhs_change",
    schema="analysis",
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
