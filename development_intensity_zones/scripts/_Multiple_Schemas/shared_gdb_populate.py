"""
This script populates a GDB on the DVRPC file system (therefore a shared GDB) with the main DIZ output feature classes from the Postgres DB
"""

import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


schema_names = [
    "_raw",
    "analysis",
    "analysis",
    "analysis",
    "analysis",
    "analysis",
    "analysis",
]  # Makes a list of the feature class' schema names (update as accordingly when a new feature class needs to get exported)

fc_names = [
    "pos_h2o_diz_zone_0",
    "diz_block_group",
    "diz_mcd",
    "diz_philadelphia_planning_district",
    "diz_taz",
    "diz_tract",
    "diz",
]  # Makes a list of the feature class' names (update as accordingly when a new feature class needs to get exported)


for i in list(range(len(schema_names))):
    fc = db.get_geodataframe_from_query(
        f"SELECT * FROM {schema_names[i]}.{fc_names[i]}"
    )  # Reads in the feature class

    fc = fc[~fc.is_empty]  # Drops any empty geometries if there are any

    if (any("MultiPolygon" in i for i in list(fc.geom_type.unique()))) & (
        len(list(fc.geom_type.unique())) > 1
    ):  # Checks if any geometries in the feature class are multi-polygons AND there are at least 2 types of geometries in the feature class
        fc = fc.explode(
            ignore_index=True
        )  # Turns multipolygon geometries into regular polygon geometries ONLY if any geometries in the feature class are multi-polygons

    else:
        pass

    fc.to_file(
        "U:/_OngoingProjects/Development_Intensity_Zones/project_output/diz_results.gdb",
        layer=fc_names[i],
    )  # Exports the feature class to the shared GDB (therefore also as a feature class). AND IGNORE ANY WARNING MESSAGES THAT COME UP HERE, THIS COMMAND WORKS
