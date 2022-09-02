"""
This script uploads the raw input data that's needed to make the VisionEval (VE)-State typology method into _raw
"""

import pandas as pd

import geopandas as gpd

import gzip

import requests

import io

import fiona

import zipfile

import tempfile

from collections import Counter


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


dvrpc_shplist = [
    gpd.read_file(
        "https://arcgis.dvrpc.org/portal/rest/services/Planning/DVRPC_LandUse_2015/FeatureServer/0/query?f=geojson&where=(lu15sub%20IN%20(%2713000%27))&outFields=*"
    ),
    gpd.read_file(
        "P:/15-44-070 Environmental Planning/GIS Data/Open Space Inventory/Final Layers POS 2020/2020_CompleteRegion_POS.shp"
    ),
]  # Streams in as geo data frames/shapefiles: A FILTERED GIS server link containing just the records I want from the 2015 DVRPC land use inventory, and the entire 2020 DVRPC
# protected open space inventory, and puts both of them into a list

dvrpc_shplist = [
    i.explode(ignore_index=True) for i in dvrpc_shplist
]  # Turns multipolygon geometries into regular polygon geometries in both geo data frames/shapefiles. This solves errors when importing the geo data frames/shapefiles into the
# database later

dvrpc_shplist = [
    i.to_crs(crs="EPSG:32618") for i in dvrpc_shplist
]  # Puts both of those shapefiles in the standard DVRPC EPSG if they aren't in it already

dvrpc_shpkeylist = [
    "dvrpc_land_use_2015_lu15sub_13000",
    "dvrpc_pos_2020",
]  # Creates the keys for the dictionary


pos_gdb_shpkeylist = fiona.listlayers(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/Shapes/POS.gdb"
)  # Gets just the names of each feature class/the keys for the dictionary by simply getting the name of each feature class in the POS geodatabase

pos_gdb_shplist = [
    gpd.read_file(
        "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/Shapes/POS.gdb",
        layer=feature_class,
    )
    for feature_class in pos_gdb_shpkeylist
]  # Reads in all of the feature classes in the POS geodatabase as a list of geo data frame/shapefiles

pos_gdb_shplist = [
    i.to_crs(crs="EPSG:32618") for i in pos_gdb_shplist
]  # Puts all of those feature classes in the standard DVRPC EPSG if they aren't already

pos_gdb_shplist = [
    i.explode(ignore_index=True) for i in pos_gdb_shplist
]  # Turns multipolygon geometries into regular polygon geometries in all of the geo data frames/shapefiles


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
    i.to_crs(crs="EPSG:32618") for i in blocks_and_bgs_shplist
]  # Puts all of those shapefiles in the standard DVRPC EPSG

blocks_and_bgs_shplist = [
    i.explode(ignore_index=True) for i in blocks_and_bgs_shplist
]  # Turns multipolygon geometries into regular polygon geometries in all of the geo data frames/shapefiles

blocks_and_bgs_shps_shpkeylist = [
    re.sub(".*\/|\..*", "", i) for i in blocks_and_bgs_shps_urls
]  # Gets the keys for the dictionary by extracting just the shapefile names from blocks_and_bgs_shps_urls

"""
blocks_and_bgs_shps_shpkeylist = [
    re.sub(".*\\\|\.shp", "", i)
    for i in [
        tempfile.gettempdir() + "\\" + i
        for i in [x for x in os.listdir(tempfile.gettempdir()) if x.endswith(".shp")]
    ]
]  # Gets just the name of each shapefile/the keys for the dictionary by extracting the name of each shapefile from the file name by getting rid 
# of the ".shp"'s from each file name (NOTE THAT THE KEYS CREATION HAS TO BE DONE THIS WAY AS OPPOSED TO BASED OFF THE URLS IN THIS CASE, SINCE THE ORDER THE SHAPEFILES DOWNLOAD 
# IN VARY FROM THE WAY THEY'RE ORDERED IN THE blocks_and_bgs_shps_urls LIST)
"""


keys_for_nonspatial_tables_to_upload_dictionary = {
    k: v for (k, v) in locals().items() if "_tablekeylist" in k
}  # First puts the lists that contain the keys for the eventual dictionary of non-spatial tables to upload to the database/the eventual non-spatial table names into their own
# dictionary

keys_for_nonspatial_tables_to_upload_dictionary = [
    s for l in list(keys_for_nonspatial_tables_to_upload_dictionary.values()) for s in l
]  # Then makes that dictionary's values into a big list of all the keys, thereby getting the final list of keys for the eventual dictionary of non-spatial tables to upload to
# the database/ the eventual non-spatial table names

values_for_nonspatial_tables_to_upload_dictionary = {
    k: v for (k, v) in locals().items() if "_tablelist" in k
}  # This and the next command then repeat the process, but for the values for the eventual dictionary of non-spatial tables to upload to the database/the eventual non-spatial
# table names

values_for_nonspatial_tables_to_upload_dictionary = [
    s for l in list(values_for_nonspatial_tables_to_upload_dictionary.values()) for s in l
]

nonspatial_tables_to_upload_dictionary = dict(
    zip(
        keys_for_nonspatial_tables_to_upload_dictionary,
        values_for_nonspatial_tables_to_upload_dictionary,
    )
)  # Creates the dictionary of tables to upload to the database/the eventual table names

for tablename, df in nonspatial_tables_to_upload_dictionary.items():
    db.import_dataframe(
        df, f"_raw.{tablename}", df_import_kwargs={"if_exists": "replace", "index": False}
    )  # For each table in nonspatial_tables_to_upload_dictionary, exports it to _raw

keys_for_shps_to_upload_dictionary = {
    k: v for (k, v) in locals().items() if "_shpkeylist" in k
}  # This and the next 5 commands essentially repeat the process, but for spatial tables/geo data frames/shapefiles

keys_for_shps_to_upload_dictionary = [
    s for l in list(keys_for_shps_to_upload_dictionary.values()) for s in l
]

values_for_shps_to_upload_dictionary = {k: v for (k, v) in locals().items() if "_shplist" in k}

values_for_shps_to_upload_dictionary = [
    s for l in list(values_for_shps_to_upload_dictionary.values()) for s in l
]

shps_to_upload_dictionary = dict(
    zip(
        keys_for_shps_to_upload_dictionary,
        values_for_shps_to_upload_dictionary,
    )
)

db.import_geodataframe(
    values_for_shps_to_upload_dictionary[2], keys_for_shps_to_upload_dictionary[2], schema="_raw"
)  # COME BACK HERE WHEN I COME BACK TO THIS TO CONTINUE EXPLORING THE CAUSES OF ERRORS HERE

"""
for tablename, df in shps_to_upload_dictionary.items():
    db.import_geodataframe(df, f"{tablename}", schema="_raw")
"""
