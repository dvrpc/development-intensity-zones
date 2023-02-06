"""
This script uploads the raw input data that's needed to make the VisionEval (VE)-State typology method into _raw
"""

import pandas as pd

import geopandas as gpd

import requests

import io

import zipfile

import tempfile

from shapely.geometry import Polygon, MultiPolygon, shape, Point


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


from typology_experiments.helpers.helper_functions import *  # Imports all functions from helper_functions.py. THIS MUST ALWAYS COME AFTER THE

# "from typology_experiments import Database, DATABASE_URL" AND "db = Database(DATABASE_URL)" COMMANDS IN THAT ORDER


tot_pops_and_hhs_2020_block = stream_in_census_data(
    "https://api.census.gov/data/2020/dec/pl?get=P1_001N,P5_001N,H1_001N,H1_002N&for=block:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="
    + os.environ["CENSUS_KEY"],
    ["state", "county", "tract", "block"],
)  # Uses my function to stream in the 2020 total populaion, group quarters population, total households, and total housing units by BLOCK for all of DE, MD, NJ and PA census table I want using the
# census API

tot_pops_and_hhs_2020_block = tot_pops_and_hhs_2020_block[
    ["state", "county", "tract", "block", "P1_001N", "P5_001N", "H1_001N", "H1_002N"]
]  # Reorders the columns

tot_pops_and_hhs_2020_block.insert(
    3, "block_1std", tot_pops_and_hhs_2020_block["block"].str[0]
)  # Gets the first digit of each block ID, puts those values into a new column, and puts that column to the left of block

tot_pops_and_hhs_2020_block.insert(
    4,
    "block_group20",
    tot_pops_and_hhs_2020_block["state"]
    + tot_pops_and_hhs_2020_block["county"]
    + tot_pops_and_hhs_2020_block["tract"]
    + tot_pops_and_hhs_2020_block["block_1std"],
)  # Gets each block's block group ID, puts those values into a new column, and puts that column to the right of block_1std

tot_pops_and_hhs_2020_bg = stream_in_census_data(
    "https://api.census.gov/data/2020/dec/pl?get=P1_001N,P5_001N,H1_001N,H1_002N&for=block%20group:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="
    + os.environ["CENSUS_KEY"],
    ["state", "county", "tract", "block group"],
)  # Uses my function to stream in the 2020 total populaion, group quarters population, total households, and total housing units by BLOCK GROUP for all of DE, MD, NJ and PA census table I want using
# the census API

tot_pops_and_hhs_2020_bg = tot_pops_and_hhs_2020_bg[
    ["state", "county", "tract", "block group", "P1_001N", "P5_001N", "H1_001N", "H1_002N"]
]  # Reorders the columns

tot_pops_and_hhs_2020_tablelist = [
    tot_pops_and_hhs_2020_block,
    tot_pops_and_hhs_2020_bg,
]  # Puts both tables into a list

tot_pops_and_hhs_2020_tablekeylist = [
    "tot_pops_and_hhs_2020_block",
    "tot_pops_and_hhs_2020_bg",
]  # Manually creates the keys for the dictionary


nonspatial_tables_to_upload_dictionary = dict(
    zip(
        tot_pops_and_hhs_2020_tablekeylist,
        tot_pops_and_hhs_2020_tablelist,
    )
)  # Creates the dictionary of tables to upload to the database/the eventual table names

[
    db.import_dataframe(
        df, f"_raw.{tablename}", df_import_kwargs={"if_exists": "replace", "index": False}
    )
    for tablename, df in nonspatial_tables_to_upload_dictionary.items()
]  # For each table in nonspatial_tables_to_upload_dictionary, exports it to _raw


def convert_3D_2D(geometry):
    new_geo = []
    for p in geometry:
        if p.has_z:
            if p.geom_type == "Polygon":
                lines = [xy[:2] for xy in list(p.exterior.coords)]
                new_p = Polygon(lines)
                new_geo.append(new_p)
            elif p.geom_type == "MultiPolygon":
                new_multi_p = []
                for ap in p:
                    lines = [xy[:2] for xy in list(ap.exterior.coords)]
                    new_p = Polygon(lines)
                    new_multi_p.append(new_p)
                new_geo.append(MultiPolygon(new_multi_p))
    return new_geo


pos_h2o_transect_zone_0 = gpd.GeoDataFrame.from_file(
    "U:_OngoingProjects/CoStar/To be moved later/Transect for AGO/db_exports.gdb",
    layer="pos_h2o_transect_zone_0",
)  # Reads in the POS and water data for all counties to use directly from the geodatabase it's stored in, and Gets rid of the Z-geometries from it

pos_h2o_transect_zone_0 = pos_h2o_transect_zone_0.to_crs(
    26918
)  # Puts it in the standard DVRPC EPSG in case it isn't in it already


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
    i.to_crs(26918) for i in blocks_and_bgs_shplist
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
        ["pos_h2o_transect_zone_0"] + blocks_and_bgs_shpkeylist,
        [pos_h2o_transect_zone_0] + blocks_and_bgs_shplist,
    )
)  # This and the next command repeat the process that was used for the non-spatial tables, but for the spatial tables/geo data frames/shapefiles

[
    db.import_geodataframe(df, f"{tablename}", schema="_raw")
    for tablename, df in shps_to_upload_dictionary.items()
]
