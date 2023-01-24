"""
This script creates analysis.transect_mcd_and_district_translation
"""


import pandas as pd

import numpy as np

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


block2020_parent_geos_philly = db.get_dataframe_from_query(
    "SELECT * FROM _resources.block2020_parent_geos WHERE mcd_name = 'Philadelphia City'"
)  # Uses my function to bring in _resources.block2020_parent_geos

phila_block_group_ids = "', '".join(
    list(block2020_parent_geos_philly["block_group20_id"])
)  # Gets a list of the IDs of Philadelphia's block groups and puts them into 1 long string, where each block group ID is separated by a "', '"

transect_philly = db.get_dataframe_from_query(
    "SELECT block_group20 AS block_group20_id, transect_zone FROM analysis.transect WHERE block_group20 IN ('"
    + phila_block_group_ids
    + "')"
)  # Uses my function to bring in a NON-SPATIAL analysis.transect, and with just the columns I want from it, with the names I want them to have, and just the records I want from it


block2020_parent_geos_philly = pd.merge(
    block2020_parent_geos_philly, transect_philly, on=["block_group20_id"], how="left"
)  # Left joins transect_philly to block2020_parent_geos_philly

transect_weighted_averages = pd.DataFrame(
    block2020_parent_geos_philly.groupby(block2020_parent_geos_philly["district_id"])
    .apply(lambda x: np.average(x["transect_zone"], weights=x["aland"]))
    .reset_index(level=0)
    .rename(columns={0: "transect_weighted_average"})
)  # For each district_id value in block2020_parent_geos, gets an average of transect_zone WEIGHTED BY aland, and stores the results in a new data frame called transect_weighted_averages

transect_weighted_averages["transect_zone"] = round(
    transect_weighted_averages["transect_weighted_average"], 0
)  # Creates a new column called transect_zone which rounds transect_weighted_average to the nearest whole number

dvrpc_mcds_with_philly_pds = (
    gpd.read_file(
        "https://arcgis.dvrpc.org/portal/rest/services/Demographics/Census_MCDs_PhiPD_2020/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    )
    .to_crs(26918)
    .rename(columns={"geoid": "district_id"})
    .explode(index_parts=False)
)  # Streams in a GeoJSON of the DVRPC's 9 counties' MCDs, with Philly represented as a collection of its planning districts

transect_mcd_and_district_translation = dvrpc_mcds_with_philly_pds.merge(
    transect_weighted_averages, on=["district_id"], how="left"
)  # Left joins transect_weighted_averages to dvrpc_mcds_with_philly_pds, and stores the result in a new object called transect_mcd_and_district_translation

db.import_geodataframe(
    transect_mcd_and_district_translation,
    "transect_mcd_and_district_translation",
    schema="analysis",
)  # Uploads the completed shapefile to analysis
