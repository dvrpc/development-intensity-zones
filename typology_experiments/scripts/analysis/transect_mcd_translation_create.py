"""
This script creates analysis.transect_mcd_translation. AND REMEMBER TO DELETE THE CURRENT analysis.transect_mcd_translation FIRST BEFORE RERUNNING THIS SCRIPT
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
    block2020_parent_geos.groupby(block2020_parent_geos["mcd20_id"])
    .apply(lambda x: np.average(x["transect_zone"], weights=x["aland"]))
    .reset_index(level=0)
    .rename(columns={0: "transect_weighted_average"})
)  # For each mcd20_id value in block2020_parent_geos, gets an average of transect_zone WEIGHTED BY aland, and stores the results in a new data frame called transect_weighted_averages

transect_weighted_averages["transect_zone"] = round(
    transect_weighted_averages["transect_weighted_average"], 0
)  # Creates a new column called transect_zone which rounds transect_weighted_average to the nearest whole number

dvrpc_mcds_phillyas1 = (
    gpd.read_file(
        "https://arcgis.dvrpc.org/portal/rest/services/Demographics/Census_MCDs_2020/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    )
    .to_crs(26918)
    .rename(columns={"geoid": "mcd20_id"})
    .explode(index_parts=False)
)  # Streams in a GeoJSON of the DVRPC's 9 counties' MCDs, with Philly represented as 1 big MCD

transect_mcd_translation = dvrpc_mcds_phillyas1.merge(
    transect_weighted_averages, on=["mcd20_id"], how="left"
)  # Left joins transect_weighted_averages to dvrpc_mcds_phillyas1, and stores the result in a new object called transect_mcd_translation

db.import_geodataframe(
    transect_mcd_translation,
    "transect_mcd_translation",
    schema="analysis",
)  # Uploads the completed shapefile to analysis
