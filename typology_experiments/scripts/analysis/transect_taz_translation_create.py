"""
This script creates analysis.transect_taz_translation
"""


import pandas as pd

import numpy as np

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


transect = db.get_dataframe_from_query(
    "SELECT block_group20 AS block_group20_id, transect_zone FROM analysis.transect"
)  # Uses my function to bring in a NON-SPATIAL analysis.transect, and with just the columns I want from it, and with the names I want them to have

block2020_parent_geos = db.get_dataframe_from_query(
    "SELECT * FROM _resources.block2020_parent_geos"
)  # Uses my function to bring in _resources.block2020_parent_geos


block2020_parent_geos = pd.merge(
    block2020_parent_geos, transect, on=["block_group20_id"], how="left"
)  # Left joins transect to block2020_parent_geos

transect_weighted_averages = pd.DataFrame(
    block2020_parent_geos.groupby(block2020_parent_geos["taz_id"])
    .apply(lambda x: np.average(x["transect_zone"], weights=x["aland"]))
    .reset_index(level=0)
    .rename(columns={0: "transect_weighted_average"})
)  # For each taz_id value in block2020_parent_geos, gets an average of transect_zone WEIGHTED BY aland, and stores the results in a new data frame called transect_weighted_averages

transect_weighted_averages["transect_zone"] = round(
    transect_weighted_averages["transect_weighted_average"], 0
)  # Creates a new column called transect_zone which rounds transect_weighted_average to the nearest whole number

dvrpc_tazs = (
    gpd.read_file(
        "https://arcgis.dvrpc.org/portal/rest/services/Demographics/TAZ_2010/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    )
    .to_crs(26918)
    .rename(columns={"taz_2010": "taz_id"})
)  # Streams in a GeoJSON of the DVRPC's 9 counties' TAZs

transect_taz_translation = dvrpc_tazs.merge(
    transect_weighted_averages, on=["taz_id"], how="left"
)  # Left joins transect_weighted_averages to dvrpc_tazs, and stores the result in a new object called transect_taz_translation


db.execute(
    "DROP TABLE IF EXISTS analysis.transect_taz_translation"
)  # First deletes the current analysis.transect_taz_translation (the import_geodataframe() function doesn't replace whatever's currently there for some reason)

db.import_geodataframe(
    transect_taz_translation,
    "transect_taz_translation",
    schema="analysis",
)  # Then uploads the completed transect_taz_translation shapefile to analysis
