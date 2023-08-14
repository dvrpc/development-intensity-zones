"""
This script uploads _raw.pos_h2o_diz_zone_0. ALSO, REMEMBER THAT THIS SCRIPT IS ONLY MEANT TO BE RUN JUST ONCE AND THAT'S IT
"""

import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


pos_h2o_diz_zone_0 = gpd.read_file(
    "U:_OngoingProjects/CoStar/To be moved later/Transect for AGO/db_exports.gdb",
    layer="pos_h2o_transect_zone_0_v2",
)  # Brings in the POS and water feature class Sean Lawrence repaired

pos_h2o_diz_zone_0 = pos_h2o_diz_zone_0.explode(
    ignore_index=True
)  # Turns the multipolygon geometries into regular polygon geometries


"""
db.execute(
    "TRUNCATE _raw.pos_h2o_diz_zone_0"
)  # First wipes out any records in _raw.pos_h2o_diz_zone_0
"""

db.import_dataframe(
    pos_h2o_diz_zone_0,
    "_raw.pos_h2o_diz_zone_0",
    df_import_kwargs={"if_exists": "append", "index": False},
)  # Then repopulates _raw.pos_h2o_diz_zone_0 with the new records
