"""
This script uploads the raw input data that's needed to make the VisionEval (VE)-State typology method into _raw
"""

proj_library_folder_path = (
    "C:/Users/ISchwarzenberg/Miniconda3/envs/tracking-progress/Library/share/proj"
)


import pandas as pd

import geopandas as gpd

import esri2gpd  # FIRST HAD TO RUN "conda activate typology-experiments" AND THEN 'python -m pip install "esri2gpd"' BEFORE RUNNING THIS COMMAND


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


b  # Get LODES7 2018 by 2010 block by Workplace (WAC) (including the total employment in all sectors column - the "JT00" GZ files have all job types) (get from https://lehd.ces.census.gov/data/#lodes )


dvrpc_land_use_2015 = esri2gpd.get(
    "https://arcgis.dvrpc.org/portal/rest/services/Planning/DVRPC_LandUse_2015/FeatureServer/0",
    where="lu15sub = '13000'",
).to_crs(
    crs="EPSG:32618"
)  # FIX THE ERROR THAT'S HERE WHEN I COME BACK HERE: Reads in as a geo data frame/shapefile in the standard DVRPC EPSG a GIS server link containing just the records I want from the 2015 DVRPC land use inventory


b  # Bring in entire 2020 dvrpc protected open space inventory, located at P:\15-44-070 Environmental Planning\GIS Data\Open Space Inventory\Final Layers POS 2020\2020_CompleteRegion_POS.shp


b  # Bring in the features from  G:\Shared drives\Long Range Plan\2050B Plan\Centers Update\typology_experiments\Shapes\POS.gdb


b  # Bring in DVRPC block group polygons?


b  # Load all the data I gathered above into the typology_experiments Postgres database
