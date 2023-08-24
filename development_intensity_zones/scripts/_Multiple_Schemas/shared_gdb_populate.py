"""
This script exports _raw.pos_h2o_diz_zone_0 and analysis.diz_block_group as feature classes to a GDB on the file system
"""

import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


schema_names = ["_raw", "analysis", "analysis"]  # Makes a list of each feature class's schema name

fc_names = [
    "pos_h2o_diz_zone_0",
    "diz_block_group",
    "diz",
]  # Makes a list of each feature class's name

[
    db.get_geodataframe_from_query(f"SELECT * FROM {schema_names[i]}.{fc_names[i]}").to_file(
        "U:/_OngoingProjects/Development_Intensity_Zones/project_output/diz_results.gdb",
        layer=fc_names[i],
    )
    for i in list(range(len(schema_names)))
]  # For each feature class, reads it in and then exports it to the geodatabase as a feature class. AND IGNORE ANY WARNING MESSAGES THAT COME UP HERE, THIS COMMAND WORKS
