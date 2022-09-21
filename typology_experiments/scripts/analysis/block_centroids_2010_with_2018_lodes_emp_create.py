"""
This script creates analysis.block_centroids_2010_with_2018_lodes_emp_create
"""

import pandas as pd

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


blocks_all4states_2010 = db.get_geodataframe_from_query(
    "SELECT * FROM _raw.tl_2010_10_tabblock10 UNION SELECT * FROM _raw.tl_2010_24_tabblock10 UNION SELECT * FROM _raw.tl_2010_34_tabblock10 UNION SELECT * FROM _raw.tl_2010_42_tabblock10"
)  # Uses my function to bring in the 2010 Delaware, Maryland, New Jersey and Pennsylvania blocks shapefiles, and combine them into 1 big one

blocks_all4states_2010_centroids = (
    blocks_all4states_2010.copy()
)  # Begins the process of getting all the blocks' centroids

blocks_all4states_2010_centroids["geom"] = blocks_all4states_2010_centroids[
    "geom"
].centroid  # Finishes the process of getting all the blocks' centroids


lodes_all4states_2018 = db.get_dataframe_from_query(
    'SELECT * FROM _raw."de_wac_S000_JT00_2018" UNION SELECT * FROM _raw."md_wac_S000_JT00_2018" UNION SELECT * FROM _raw."nj_wac_S000_JT00_2018" UNION SELECT * FROM _raw."pa_wac_S000_JT00_2018"'
)  # Uses my function to bring in the 2018 Delaware, Maryland, New Jersey and Pennsylvania LODES tables, and combine them into 1 big one

lodes_all4states_2018.insert(
    1, "GEOID10", lodes_all4states_2018["w_geocode"].apply(str)
)  # Makes a string column of w_geocode to get each block's GEOID10 value, and puts it to the right of w_geocode


block_centroids_2010_with_2018_lodes_emp = blocks_all4states_2010_centroids.merge(
    lodes_all4states_2018, on=["GEOID10"], how="left"
)  # Left joins lodes_all4states_2018 to blocks_all4states_2010_centroids to create block_centroids_2010_with_2018_lodes_emp

block_centroids_2010_with_2018_lodes_emp = block_centroids_2010_with_2018_lodes_emp[
    [
        "STATEFP10",
        "COUNTYFP10",
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
        "w_geocode",
        "c000",
        "ca01",
        "ca02",
        "ca03",
        "ce01",
        "ce02",
        "ce03",
        "cns01",
        "cns02",
        "cns03",
        "cns04",
        "cns05",
        "cns06",
        "cns07",
        "cns08",
        "cns09",
        "cns10",
        "cns11",
        "cns12",
        "cns13",
        "cns14",
        "cns15",
        "cns16",
        "cns17",
        "cns18",
        "cns19",
        "cns20",
        "cr01",
        "cr02",
        "cr03",
        "cr04",
        "cr05",
        "cr07",
        "ct01",
        "ct02",
        "cd01",
        "cd02",
        "cd03",
        "cd04",
        "cs01",
        "cs02",
        "cfa01",
        "cfa02",
        "cfa03",
        "cfa04",
        "cfa05",
        "cfs01",
        "cfs02",
        "cfs03",
        "cfs04",
        "cfs05",
        "createdate",
        "geom",
        "uid",
    ]
]  # Reorders the columns

db.import_geodataframe(
    block_centroids_2010_with_2018_lodes_emp,
    "block_centroids_2010_with_2018_lodes_emp",
    schema="analysis",
)  # Uploads the completed shapefile to analysis. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
