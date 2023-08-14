"""
This script uploads _raw.pos_h2o_diz_zone_0. ALSO, REMEMBER THAT THIS SCRIPT IS ONLY MEANT TO BE RUN JUST ONCE AND THAT'S IT
"""

import geopandas as gpd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


pos_h2o_diz_zone_0 = gpd.read_file(
    "G:/Shared drives/Socioeconomic and Land Use Analytics/Tracking Progress/Data Preparation/postgres_db/pos_h2o_diz_zone_0_SHP.shp"
)  # Brings in the 2D POS and water shapefile with repaired geometries I made

pos_h2o_diz_zone_0 = pos_h2o_diz_zone_0.explode(
    ignore_index=True
)  # Turns multipolygon geometries into regular polygon geometries


db.execute(
    "TRUNCATE _raw.pos_h2o_diz_zone_0"
)  # First wipes out any records in _raw.pos_h2o_diz_zone_0

db.import_dataframe(
    pos_h2o_diz_zone_0,
    "_raw.pos_h2o_diz_zone_0",
    df_import_kwargs={"if_exists": "append", "index": False},
)  # Then repopulates _raw.pos_h2o_diz_zone_0 with the new records
