"""
This script uploads _raw.pos_h2o_diz_zone_0. ALSO, REMEMBER THAT THIS SCRIPT IS ONLY MEANT TO BE RUN JUST ONCE AND THAT'S IT
"""

import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


pos_h2o_diz_zone_0 = gpd.read_file(
    "U:/_OngoingProjects/CoStar/To be moved later/Transect for AGO/pos_source.gdb",
    layer="pos_union_wDVRPC_hydro1_dissolve",
)  # Brings in the POS and water feature class Sean Lawrence made


db.execute(
    "TRUNCATE _raw.pos_h2o_diz_zone_0"
)  # First wipes out any records in _raw.pos_h2o_diz_zone_0

db.import_geodataframe(
    pos_h2o_diz_zone_0, "pos_h2o_diz_zone_0", schema="_raw"
)  # Then repopulates _raw.pos_h2o_diz_zone_0 with the new record(s)
