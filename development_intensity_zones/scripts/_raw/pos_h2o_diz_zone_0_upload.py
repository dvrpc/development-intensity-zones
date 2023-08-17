"""
This script uploads _raw.pos_h2o_diz_zone_0. ALSO, REMEMBER THAT THIS SCRIPT IS ONLY MEANT TO BE RUN JUST ONCE AND THAT'S IT
"""

import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


pos_h2o_diz_zone_0 = gpd.read_file(
    "U:/_OngoingProjects/CoStar/To be moved later/Transect for AGO/pos_source.gdb",
    layer="pos_h2o_exploded",
)  # Brings in the POS and water feature class Sean Lawrence made

pos_h2o_diz_zone_0.insert(
    0, "dissolve_field", 1
)  # This and the next command dissolve all the individual polygons in pos_h2o_diz_zone_0 so that it's just 1 big polygon of POS and water

pos_h2o_diz_zone_0 = pos_h2o_diz_zone_0.dissolve(by="dissolve_field")


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
