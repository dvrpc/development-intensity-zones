"""
This script uploads the raw input data that's needed to make the VisionEval (VE)-State typology method into _raw
"""

import pandas as pd

import geopandas as gpd

import fiona

import gzip

import requests

import io

import zipfile

import tempfile


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


from typology_experiments.helpers.helper_functions import *  # Imports all functions from helper_functions.py. THIS MUST ALWAYS COME AFTER THE

# "from typology_experiments import Database, DATABASE_URL" AND "db = Database(DATABASE_URL)" COMMANDS IN THAT ORDER


tot_pops_and_hhs_2020_block = stream_in_census_data(
    "https://api.census.gov/data/2020/dec/pl?get=P1_001N,P5_001N,H1_002N&for=block:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="
    + os.environ["CENSUS_KEY"],
    ["state", "county", "tract", "block"],
)  # Uses my function to stream in the 2020 total populaion, group quarters population, and total households by BLOCK for all of DE, MD, NJ and PA census table I want using the
# census API

tot_pops_and_hhs_2020_block = tot_pops_and_hhs_2020_block[
    ["state", "county", "tract", "block", "P1_001N", "P5_001N", "H1_002N"]
]  # Reorders the columns

tot_pops_and_hhs_2020_bg = stream_in_census_data(
    "https://api.census.gov/data/2020/dec/pl?get=P1_001N,P5_001N,H1_002N&for=block%20group:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="
    + os.environ["CENSUS_KEY"],
    ["state", "county", "tract", "block group"],
)  # Uses my function to stream in the 2020 total populaion, group quarters population, and total households by BLOCK GROUP for all of DE, MD, NJ and PA census table I want using
# the census API

tot_pops_and_hhs_2020_bg = tot_pops_and_hhs_2020_bg[
    ["state", "county", "tract", "block group", "P1_001N", "P5_001N", "H1_002N"]
]  # Reorders the columns

tot_pops_and_hhs_2020_tablelist = [
    tot_pops_and_hhs_2020_block,
    tot_pops_and_hhs_2020_bg,
]  # Puts both tables into a list

tot_pops_and_hhs_2020_tablekeylist = [
    "tot_pops_and_hhs_2020_block",
    "tot_pops_and_hhs_2020_bg",
]  # Manually creates the keys for the dictionary


lodes7_tables_urls = [
    "https://lehd.ces.census.gov/data/lodes/LODES7/de/wac/de_wac_S000_JT00_2018.csv.gz",
    "https://lehd.ces.census.gov/data/lodes/LODES7/md/wac/md_wac_S000_JT00_2018.csv.gz",
    "https://lehd.ces.census.gov/data/lodes/LODES7/nj/wac/nj_wac_S000_JT00_2018.csv.gz",
    "https://lehd.ces.census.gov/data/lodes/LODES7/pa/wac/pa_wac_S000_JT00_2018.csv.gz",
]  # Creates a list of the URLs to use to get the LODES7 "wac_S000_JT00_2018" CSV GZ files for each state (DE, MD, NJ and PA)

lodes7_tablelist = [
    pd.read_csv(gzip.open(io.BytesIO(requests.get(i).content))) for i in lodes7_tables_urls
]  # Streams in those LODES7 tables from their CSV GZ files

lodes7_tablekeylist = [
    re.sub(".*\/|\..*", "", i) for i in lodes7_tables_urls
]  # Creates the keys for the dictionary by extracting just the CSV names from lodes7_tables_urls


dvrpc_forecast_2020_emp_block10_tablelist = [
    pd.read_csv(
        "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/dvrpc_forecast_2020_emp_block10.csv"
    )
]  # Reads in the DVRPC 2020 employment forecast by 2010 block data and puts it into a list

dvrpc_forecast_2020_emp_block10_tablekeylist = [
    "dvrpc_forecast_2020_emp_block10"
]  # Creates the key for that eventual dictionary


nonspatial_tables_to_upload_dictionary = dict(
    zip(
        tot_pops_and_hhs_2020_tablekeylist
        + lodes7_tablekeylist
        + dvrpc_forecast_2020_emp_block10_tablekeylist,
        tot_pops_and_hhs_2020_tablelist
        + lodes7_tablelist
        + dvrpc_forecast_2020_emp_block10_tablelist,
    )
)  # Creates the dictionary of tables to upload to the database/the eventual table names

[
    db.import_dataframe(
        df, f"_raw.{tablename}", df_import_kwargs={"if_exists": "replace", "index": False}
    )
    for tablename, df in nonspatial_tables_to_upload_dictionary.items()
]  # For each table in nonspatial_tables_to_upload_dictionary, exports it to _raw


pos_and_land_use_shpkeylist = [
    "dvrpc_2015_water",
    "dvrpc_2020_pos",
]  # Creates the keys for the dictionary

pos_and_land_use_shp_links = [
    "https://arcgis.dvrpc.org/portal/rest/services/Planning/DVRPC_LandUse_2015/FeatureServer/0/query?f=geojson&where=(lu15sub%20IN%20(%2713000%27))&outFields=*",
    "https://arcgis.dvrpc.org/portal/rest/services/Planning/DVRPC_ProtectedOpenSpace/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
]  # Creates a list that contains the links to the POS and land use shapefiles

pos_and_land_use_shplist = [
    gpd.read_file(i) for i in pos_and_land_use_shp_links
]  # Streams in as geo data frames/shapefiles those links and puts them into a list

pos_and_land_use_shplist = [
    i.explode(ignore_index=True) for i in pos_and_land_use_shplist
]  # Turns multipolygon geometries into regular polygon geometries in both geo data frames/shapefiles. This solves errors when importing the geo data frames/shapefiles into the
# database later

pos_and_land_use_shplist = [
    i.to_crs(crs="EPSG:32618") for i in pos_and_land_use_shplist
]  # Puts both of those shapefiles in the standard DVRPC EPSG if they aren't in it already


blocks_and_bgs_shps_urls = [
    "https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_10_tabblock10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_24_tabblock10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_34_tabblock10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_42_tabblock10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_10_tabblock20.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_24_tabblock20.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_34_tabblock20.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_42_tabblock20.zip",
    "https://www2.census.gov/geo/tiger/TIGER2010/BG/2010/tl_2010_10_bg10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2010/BG/2010/tl_2010_24_bg10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2010/BG/2010/tl_2010_34_bg10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2010/BG/2010/tl_2010_42_bg10.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/BG/tl_2020_10_bg.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/BG/tl_2020_24_bg.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/BG/tl_2020_34_bg.zip",
    "https://www2.census.gov/geo/tiger/TIGER2020/BG/tl_2020_42_bg.zip",
]  # Generates the list of URLs where the 2010 and 2020 statewide block and block groups Census TIGER/Line shapefiles can be found

[
    zipfile.ZipFile(io.BytesIO(requests.get(i).content)).extractall(path=tempfile.gettempdir())
    for i in blocks_and_bgs_shps_urls
]  # This and the next command stream in all of Delaware, Maryland, New Jersey and Pennsylvania's 2010 and 2020 BLOCK and BLOCK GROUP polygons. ALSO IGNORE THE
# "[None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]" THAT GETS PRINTED HERE, THIS COMMAND WORKS

blocks_and_bgs_shplist = [
    gpd.read_file(i)
    for i in [
        tempfile.gettempdir() + "\\" + i
        for i in [x for x in os.listdir(tempfile.gettempdir()) if x.endswith(".shp")]
    ]
]

blocks_and_bgs_shplist = [
    i.explode(ignore_index=True) for i in blocks_and_bgs_shplist
]  # Turns multipolygon geometries into regular polygon geometries in all of the geo data frames/shapefiles

blocks_and_bgs_shplist = [
    i.to_crs(crs="EPSG:32618") for i in blocks_and_bgs_shplist
]  # Puts all of those shapefiles in the standard DVRPC EPSG

blocks_and_bgs_shpkeylist = [
    re.sub(".*\\\|\.shp", "", i)
    for i in [
        tempfile.gettempdir() + "\\" + i
        for i in [x for x in os.listdir(tempfile.gettempdir()) if x.endswith(".shp")]
    ]
]  # Gets just the name of each shapefile/the keys for the dictionary by extracting the name of each shapefile from the file name by getting rid of the ".shp"'s from each file
# name (NOTE THAT THE KEYS CREATION HAS TO BE DONE THIS WAY AS OPPOSED TO BASED OFF THE URLS IN THIS CASE, SINCE THE ORDER THE SHAPEFILES DOWNLOAD IN VARY FROM THE WAY THEY'RE
# ORDERED IN THE blocks_and_bgs_shps_urls LIST)


shps_to_upload_dictionary = dict(
    zip(
        pos_and_land_use_shpkeylist + blocks_and_bgs_shpkeylist,
        pos_and_land_use_shplist + blocks_and_bgs_shplist,
    )
)  # This and the next command repeat the process that was used for the non-spatial tables, but for the spatial tables/geo data frames/shapefiles

[
    db.import_geodataframe(df, f"{tablename}", schema="_raw")
    for tablename, df in shps_to_upload_dictionary.items()
]
