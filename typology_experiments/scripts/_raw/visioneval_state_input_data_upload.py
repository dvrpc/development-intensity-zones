"""
This script uploads the raw input data that's needed to make the VisionEval (VE)-State typology method into _raw
"""

import pandas as pd

import geopandas as gpd

import gzip

import requests

import io


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


from typology_experiments.helpers.helper_functions import *  # Imports all functions from helper_functions.py. THIS MUST ALWAYS COME AFTER THE

# "from typology_experiments import Database, DATABASE_URL" AND "db = Database(DATABASE_URL)" COMMANDS IN THAT ORDER


api_urls = [
    "https://api.census.gov/data/2020/dec/pl?get=P1_001N,H1_002N&for=block:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="
    + os.environ["CENSUS_KEY"],
    "https://api.census.gov/data/2020/dec/pl?get=P1_001N,H1_002N&for=block%20group:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="
    + os.environ["CENSUS_KEY"],
]  # Creates both the 2020 Decennial Census total households and population by 2020 block API URL to use, and the exact same API URL but by 2020 block group instead, and both for
# ALL of DE, MD, NJ and PA

streaming_results_list = [
    stream_in_census_data(i, ["state", "county", "tract"]) for i in api_urls
]  # Uses my function to stream in the census table I want using the census API. And note that because each table has a different column name for block


lodes7_urls = [
    "https://lehd.ces.census.gov/data/lodes/LODES7/de/wac/de_wac_S000_JT00_2018.csv.gz",
    "https://lehd.ces.census.gov/data/lodes/LODES7/md/wac/md_wac_S000_JT00_2018.csv.gz",
    "https://lehd.ces.census.gov/data/lodes/LODES7/nj/wac/nj_wac_S000_JT00_2018.csv.gz",
    "https://lehd.ces.census.gov/data/lodes/LODES7/pa/wac/pa_wac_S000_JT00_2018.csv.gz",
]  # Creates a list of the URLs to use to get the LODES7 "wac_S000_JT00_2018" CSV GZ files for each state (DE, MD, NJ and PA)

lodes7_data_list = [
    pd.read_csv(gzip.open(io.BytesIO(requests.get(i).content))) for i in lodes7_urls
]  # Streams in those LODES7 tables from their CSV GZ files


dvrpc_land_use_2015 = gpd.read_file(
    "https://arcgis.dvrpc.org/portal/rest/services/Planning/DVRPC_LandUse_2015/FeatureServer/0/query?f=geojson&where=(lu15sub%20IN%20(%2713000%27))&outFields=*"
).to_crs(
    crs="EPSG:32618"
)  # Reads in as a geo data frame/shapefile in the standard DVRPC EPSG a FILTERED GIS server link containing just the records I want from the 2015 DVRPC land use inventory


dvrpc_pos_2020 = gpd.read_file(
    "P:/15-44-070 Environmental Planning/GIS Data/Open Space Inventory/Final Layers POS 2020/2020_CompleteRegion_POS.shp"
)  # Brings in the entire 2020 DVRPC protected open space inventory


b  # Bring in the features from  G:\Shared drives\Long Range Plan\2050B Plan\Centers Update\typology_experiments\Shapes\POS.gdb


b  # Bring in DVRPC block group polygons?


b  # Load all the data I gathered above into the typology_experiments Postgres database
