"""
This script creates analysis.block_groups_24co_2020_area_calcs. THIS SCRIPT WAS ALMOST COMPLETELY WRITTEN BY SEAN LAWRENCE HERE: https://github.com/dvrpc/typology-experiments/issues/3#issuecomment-1520862620 . THE ONLY DIFFERENCES ARE THAT SEAN'S ORIGINAL SCRIPT REFERS TO _raw.pos_h2o_diz_zone_0 BY ITS OLD NAME OF _raw.pos_h2o_transect_zone_0, THIS DOESN'T, AND SEAN'S ORIGINAL SCRIPT PUTS THE DB CONNECTION INFO DIRECTLY INTO THE SCRIPT, THIS DOESN'T
"""


import geopandas as gpd
import pandas as pd
import os
from sqlalchemy import create_engine

gis_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DVRPC_GIS_POSTGRES_DB_USERNAME')}:{os.getenv('DVRPC_GIS_POSTGRES_DB_PASSWORD')}@{os.getenv('DVRPC_GIS_POSTGRES_DB_HOST')}/{os.getenv('DVRPC_GIS_POSTGRES_DB_NAME')}"
)
ian_engine = create_engine(
    f"{os.getenv('DATABASE_URL')}"
)  # In the .env file, I stored the typology-experiments DB connection info as 1 long string

# create land use dataframe from gis-db
sql_query = "select st_force2d(shape) as geom, 0 as zone from planning.dvrpc_landuse_2015 where lu15sub in ('04010','04011','04020','50000','05030','07050','09000','14020')"
lu = gpd.read_postgis(sql_query, gis_engine, geom_col="geom")

# create pos_h2o dataframe
pos_h2o = gpd.read_postgis(
    "select 0 as zone, st_force2d(geom) as geom FROM _raw.pos_h2o_diz_zone_0",
    ian_engine,
    geom_col="geom",
)

# merge landuse and pos_h2o into new dataframe
pos_h2o_lu = gpd.GeoDataFrame(
    pd.concat([lu, pos_h2o], ignore_index=True), crs=lu.crs, geometry="geom"
)

# create block group dataframe
bg = gpd.read_postgis(
    'select "GEOID", st_force2D(geom) as geom from analysis.block_groups_24co_2020',
    ian_engine,
    geom_col="geom",
)
bg["bg_area"] = bg.geometry.area

# create spatial index for both dataframes
pos_h2o_lu.sindex
bg.sindex

# calculate spatial difference of block group and pos_h2o_land use (this takes a while)
diff = bg.overlay(pos_h2o_lu, how="difference", keep_geom_type=True)

# calculate area of difference
diff["diff_area"] = diff.geometry.area

# merge area of difference with block group dataframe, calculate the percent pos_h2o_landuse in each block group
bg = bg.merge(diff[["GEOID", "diff_area"]], on="GEOID", how="left")
bg["diff_area"] = bg["diff_area"].fillna(0)
bg["percent_lu_h2o_pos"] = ((bg["bg_area"] - bg["diff_area"]) / bg["bg_area"]) * 100

# save new block group data with area calcs
bg.to_postgis(
    "block_groups_24co_2020_area_calcs",
    ian_engine,
    schema="analysis",
    if_exists="replace",
)
